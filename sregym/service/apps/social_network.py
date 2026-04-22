"""Interface to the social network application from DeathStarBench"""

import logging
import os

from sregym.generators.workload.wrk2 import Wrk2, Wrk2WorkloadManager
from sregym.observer.trace_api import TraceAPI
from sregym.paths import SOCIAL_NETWORK_METADATA, TARGET_MICROSERVICES
from sregym.service.apps.base import Application
from sregym.service.app_workspace import resolve_workspace_path
from sregym.service.apps.helpers import get_frontend_url
from sregym.service.helm import Helm
from sregym.service.kubectl import KubeCtl
from sregym.service.source_deploy import plan_for_app, source_deploy_enabled

logger = logging.getLogger("all.sregym.social_network")
logger.propagate = True
logger.setLevel(logging.DEBUG)


class SocialNetwork(Application):
    def __init__(self):
        super().__init__(SOCIAL_NETWORK_METADATA)
        self.load_app_json()
        self.kubectl = KubeCtl()
        self.trace_api = None
        self.local_tls_path = resolve_workspace_path("socialNetwork/helm-chart/socialnetwork")

        self.payload_script = resolve_workspace_path("socialNetwork/wrk2/scripts/social-network/mixed-workload.lua")

    def load_app_json(self):
        super().load_app_json()
        metadata = self.get_app_json()
        self.app_name = metadata["Name"]
        self.description = metadata["Desc"]
        self.frontend_service = metadata.get("frontend_service", "nginx-thrift")
        self.frontend_port = metadata.get("frontend_port", 8080)

    def create_docker_registry_secret(self):
        """Create docker-registry secret if DOCKER_USERNAME/PASSWORD env vars exist."""
        docker_user = os.environ.get("DOCKER_USERNAME")
        docker_password = os.environ.get("DOCKER_PASSWORD")

        if docker_user and docker_password:
            # Check if secret already exists
            check_sec = f"kubectl get secret regcred -n {self.namespace}"
            result = self.kubectl.exec_command(check_sec)

            if "regcred" not in result:
                logger.debug("Creating Docker registry secret...")
                cmd = (
                    f"kubectl create secret docker-registry regcred "
                    f"--docker-server=https://index.docker.io/v1/ "
                    f"--docker-username={docker_user} "
                    f"--docker-password={docker_password} "
                    f"--docker-email=sregym@example.com "
                    f"-n {self.namespace}"
                )
                self.kubectl.exec_command(cmd)

            # Patch default service account
            patch_cmd = (
                f"kubectl patch serviceaccount default "
                f'-p \'{{"imagePullSecrets": [{{"name": "regcred"}}]}}\' '
                f"-n {self.namespace}"
            )
            self.kubectl.exec_command(patch_cmd)
        else:
            logger.warning("DOCKER_USERNAME and DOCKER_PASSWORD env vars not found. Skipping registry secret creation.")

    def create_tls_secret(self):
        """Create TLS secret for MongoDB if it doesn't exist."""
        check_sec = f"kubectl get secret mongodb-tls -n {self.namespace}"
        result = self.kubectl.exec_command(check_sec)
        result_lower = result.lower()

        # Secret exists if we got a successful response (contains secret name without error)
        if "mongodb-tls" in result and "error" not in result_lower:
            logger.debug("TLS secret already exists. Skipping creation.")
            return

        create_sec_command = (
            f"kubectl create secret generic mongodb-tls "
            f"--from-file=tls.pem={self.local_tls_path}/tls.pem "
            f"--from-file=ca.crt={self.local_tls_path}/ca.crt "
            f"-n {self.namespace}"
        )
        create_result = self.kubectl.exec_command(create_sec_command)
        create_result_lower = create_result.lower()

        if "created" in create_result_lower:
            logger.debug(f"TLS secret created: {create_result.strip()}")
        elif "already exists" in create_result_lower:
            logger.debug("TLS secret already exists (confirmed during creation attempt).")
        else:
            logger.warning(f"TLS secret creation unexpected result: {create_result.strip()}")

    def deploy(self):
        """Deploy the Helm configurations with architecture-aware image selection."""
        self.create_namespace()
        self.configure_dockerhub_pull_secret()
        self.create_tls_secret()
        node_architectures = self.kubectl.get_node_architectures()
        is_arm = any(arch in ["arm64", "aarch64"] for arch in node_architectures)
        helm_configs = dict(self.helm_configs)
        extra_args = list(helm_configs.get("extra_args", []))

        if source_deploy_enabled():
            with plan_for_app(self, node_architectures=node_architectures) as plan:
                extra_args.extend(plan.helm_extra_args)
                helm_configs["extra_args"] = extra_args
                Helm.install(**helm_configs)
        else:
            if is_arm:
                # Use the ARM-compatible image for media-frontend.
                extra_args.append("--set media-frontend.container.image=jacksonarthurclark/media-frontend")
                extra_args.append("--set media-frontend.container.imageVersion=latest")
            if extra_args:
                helm_configs["extra_args"] = extra_args
            Helm.install(**helm_configs)
        Helm.assert_if_deployed(self.helm_configs["namespace"])
        self.trace_api = TraceAPI(self.namespace)
        self.trace_api.start_port_forward()

    def delete(self):
        """Delete the Helm configurations."""
        Helm.uninstall(**self.helm_configs)

    def cleanup(self):
        """Delete the entire namespace for the social network application."""
        if self.trace_api:
            self.trace_api.stop_port_forward()
        Helm.uninstall(**self.helm_configs)

        if hasattr(self, "wrk"):
            # self.wrk.stop()
            self.kubectl.delete_job(label="job=workload", namespace=self.namespace)
        self.kubectl.delete_namespace(self.namespace)

    def create_workload(
        self, rate: int = 100, dist: str = "exp", connections: int = 3, duration: int = 10, threads: int = 3
    ):
        self.wrk = Wrk2WorkloadManager(
            wrk=Wrk2(
                rate=rate,
                dist=dist,
                connections=connections,
                duration=duration,
                threads=threads,
                namespace=self.namespace,
            ),
            payload_script=self.payload_script,
            url="{placeholder}/wrk2-api/post/compose",
            namespace=self.namespace,
        )

    def start_workload(self):
        if not hasattr(self, "wrk"):
            self.create_workload()
        self.wrk.url = get_frontend_url(self) + "/wrk2-api/post/compose"
        self.wrk.start()

    def stop_workload(self):
        if hasattr(self, "wrk"):
            self.wrk.stop()
