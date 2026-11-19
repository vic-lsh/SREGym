import os
import logging
import shlex
from enum import StrEnum

from sregym.generators.workload.wrk2 import Wrk2, Wrk2WorkloadManager
from sregym.paths import HOTEL_RES_METADATA
from sregym.service.app_workspace import resolve_workspace_path
from sregym.service.apps.base import Application
from sregym.service.apps.helpers import get_frontend_url
from sregym.service.kubectl import KubeCtl
from sregym.service.source_deploy import plan_for_app

logger = logging.getLogger("all.application")
logger.propagate = True
logger.setLevel(logging.DEBUG)


class FailureScripts(StrEnum):
    """Historical fault-profile metadata; accepted source owns deployment bytes."""

    MOUNTED = "mounted"  # filled, and mounted at /scripts in mongodb-geo and mongodb-rate
    UNMOUNTED = "unmounted"  # filled but mounted nowhere, as robust-skills deploys every hotel app
    ABSENT = "absent"  # not created: the agent must diagnose a mongo auth fault from symptoms


class HotelReservation(Application):
    def __init__(self, failure_scripts: FailureScripts = FailureScripts.MOUNTED):
        super().__init__(HOTEL_RES_METADATA)
        self.kubectl = KubeCtl()
        self.helm_deploy = False
        self.failure_scripts = FailureScripts(failure_scripts)

        self.load_app_json()

        self.payload_script = resolve_workspace_path(
            "hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        )

    def load_app_json(self):
        super().load_app_json()
        metadata = self.get_app_json()
        self.app_name = metadata["Name"]
        self.description = metadata["Desc"]
        self.frontend_service = metadata.get("frontend_service", "frontend")
        self.frontend_port = metadata.get("frontend_port", 5000)

    def deploy(self):
        """Deploy the Kubernetes configurations."""
        self.logger.info(f"Deploying Kubernetes configurations in namespace: {self.namespace}")
        self.create_namespace()
        with plan_for_app(self) as plan:
            self.kubectl.exec_command_checked(
                f"kubectl apply -f {shlex.quote(str(plan.manifest_path))} -n {shlex.quote(self.namespace)}"
            )
        self.kubectl.wait_for_ready(self.namespace)

    def delete(self):
        """Delete the configmap."""
        self.kubectl.exec_command(f"kubectl delete -k {self.k8s_deploy_path} -n {self.namespace}")

    def cleanup(self):
        """Delete the entire namespace for the hotel reservation application."""
        self.kubectl.delete_namespace(self.namespace)

        self.kubectl.wait_for_namespace_deletion(self.namespace)
        # Match the claim's namespace exactly: a substring match would also
        # delete volumes of unrelated namespaces such as ``hotel-reservation-sdo``.
        pvs = self.kubectl.exec_command(
            'kubectl get pv -o jsonpath=\'{range .items[*]}{.metadata.name}{" "}{.spec.claimRef.namespace}{"\\n"}{end}\''
            f" | awk '$2 == \"{self.namespace}\" {{print $1}}'"
        ).splitlines()

        for pv in pvs:
            # Check if the PV is in a 'Terminating' state and remove the finalizers if necessary
            self._remove_pv_finalizers(pv)
            delete_command = f"kubectl delete pv {pv}"
            delete_result = self.kubectl.exec_command(delete_command)
            logger.info(f"Deleted PersistentVolume {pv}: {delete_result.strip()}")
        for pv in pvs:
            # Wait for the volumes to disappear instead of a fixed pause; nothing to wait for on a clean cluster.
            self.kubectl.exec_command(f"kubectl wait --for=delete pv/{pv} --timeout=60s")

        if hasattr(self, "wrk"):
            # self.wrk.stop()
            self.kubectl.delete_job(label="job=workload", namespace=getattr(self, "workload_namespace", self.namespace))
            if getattr(self, "workload_namespace", self.namespace) != self.namespace:
                self.kubectl.exec_command(
                    f"kubectl delete namespace {self.workload_namespace} --ignore-not-found --wait=false"
                )

    def _remove_pv_finalizers(self, pv_name: str):
        """Remove finalizers from the PersistentVolume to prevent it from being stuck in a 'Terminating' state."""
        # Patch the PersistentVolume to remove finalizers if it is stuck
        patch_command = f'kubectl patch pv {pv_name} -p \'{{"metadata":{{"finalizers":null}}}}\''
        _ = self.kubectl.exec_command(patch_command)

    def create_workload(
        self, rate: int = 100, dist: str = "exp", connections: int = 100, duration: int = 30, threads: int = 3
    ):
        # Benchmark hygiene: SREGYM_WORKLOAD_NAMESPACE moves the load generator out of the application namespace
        # (an agent scoped to the app namespace then sees the traffic's effects, not the generator).
        self.workload_namespace = os.environ.get("SREGYM_WORKLOAD_NAMESPACE") or self.namespace
        self.wrk = Wrk2WorkloadManager(
            wrk=Wrk2(
                rate=rate,
                dist=dist,
                connections=connections,
                duration=duration,
                threads=threads,
                namespace=self.workload_namespace,
            ),
            payload_script=self.payload_script,
            url="{placeholder}",
            namespace=self.workload_namespace,
        )

    def start_workload(self):
        if not hasattr(self, "wrk"):
            self.create_workload()
        self.wrk.url = get_frontend_url(self)
        if self.workload_namespace != self.namespace:
            self.kubectl.exec_command(
                f"kubectl create namespace {self.workload_namespace} --dry-run=client -o yaml | kubectl apply -f -"
            )
        self.wrk.start()

    def stop_workload(self):
        if hasattr(self, "wrk"):
            self.wrk.stop()
