"""Interface to the social network application from DeathStarBench"""

import logging
import shlex

from sregym.generators.workload.wrk2 import Wrk2, Wrk2WorkloadManager
from sregym.paths import SOCIAL_NETWORK_METADATA
from sregym.service.app_workspace import resolve_workspace_path
from sregym.service.apps.base import Application
from sregym.service.apps.helpers import get_frontend_url
from sregym.service.helm import Helm
from sregym.service.kubectl import KubeCtl
from sregym.service.source_deploy import plan_for_app

logger = logging.getLogger("all.sregym.social_network")
logger.propagate = True
logger.setLevel(logging.DEBUG)


class SocialNetwork(Application):
    def __init__(self):
        super().__init__(SOCIAL_NETWORK_METADATA)
        self.load_app_json()
        self.kubectl = KubeCtl()

        self.payload_script = resolve_workspace_path("socialNetwork/wrk2/scripts/social-network/mixed-workload.lua")

    def load_app_json(self):
        super().load_app_json()
        metadata = self.get_app_json()
        self.app_name = metadata["Name"]
        self.description = metadata["Desc"]
        self.frontend_service = metadata.get("frontend_service", "nginx-thrift")
        self.frontend_port = metadata.get("frontend_port", 8080)

    def deploy(self):
        """Deploy the Helm configurations with architecture-aware image selection."""
        self.create_namespace()
        with plan_for_app(self, node_architectures=self.kubectl.get_node_architectures()) as plan:
            self.kubectl.exec_command_checked(
                f"kubectl apply -f {shlex.quote(str(plan.manifest_path))} -n {shlex.quote(self.namespace)}"
            )
        Helm.assert_if_deployed(self.helm_configs["namespace"])

    def delete(self):
        """Delete the source-deployed application namespace."""
        self.kubectl.delete_namespace(self.namespace)

    def cleanup(self):
        """Delete the entire namespace for the social network application."""

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
