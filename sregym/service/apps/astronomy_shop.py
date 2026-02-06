"""Interface to the OpenTelemetry Astronomy Shop application"""

import contextlib
import fcntl
import logging
import os
import time

from sregym.generators.workload.locust import LocustWorkloadManager
from sregym.observer.trace_api import TraceAPI
from sregym.paths import ASTRONOMY_SHOP_METADATA
from sregym.service.apps.base import Application
from sregym.service.helm import Helm
from sregym.service.kubectl import KubeCtl

logger = logging.getLogger("all.service.apps.astronomy_shop")


class AstronomyShop(Application):
    def __init__(self):
        super().__init__(ASTRONOMY_SHOP_METADATA)
        self.load_app_json()
        self.kubectl = KubeCtl()
        self.trace_api = None
        self.create_namespace()

    def load_app_json(self):
        super().load_app_json()
        metadata = self.get_app_json()
        self.app_name = metadata["Name"]
        self.description = metadata["Desc"]
        self.frontend_service = "frontend-proxy"
        self.frontend_port = 8080

    def deploy(self):
        """Deploy the Helm configurations."""
        with self._deployment_lock():
            self.kubectl.create_namespace_if_not_exist(self.namespace)
            self.configure_dockerhub_pull_secret()

            # Ensure stale failed release state is cleared before a fresh install.
            Helm.uninstall(**self.helm_configs)

            self.helm_configs["extra_args"] = [
                "--set-string",
                "components.load-generator.envOverrides[0].name=LOCUST_BROWSER_TRAFFIC_ENABLED",
                "--set-string",
                "components.load-generator.envOverrides[0].value=false",
            ]

            Helm.install(**self.helm_configs)
            self.configure_dockerhub_pull_secret(patch_all_service_accounts=True, restart_pods=True)
            Helm.assert_if_deployed(self.helm_configs["namespace"])
            self.trace_api = TraceAPI(self.namespace)
            self.trace_api.start_port_forward()

    def delete(self):
        """Delete the Helm configurations."""
        Helm.uninstall(**self.helm_configs)
        self.kubectl.delete_namespace(self.helm_configs["namespace"])
        self.kubectl.wait_for_namespace_deletion(self.namespace)

    def cleanup(self):
        with self._deployment_lock():
            if self.trace_api:
                self.trace_api.stop_port_forward()
            Helm.uninstall(**self.helm_configs)
            self.kubectl.delete_namespace(self.helm_configs["namespace"])

        if hasattr(self, "wrk"):
            # self.wrk.stop()
            self.kubectl.delete_job(label="job=workload", namespace=self.namespace)

    @contextlib.contextmanager
    def _deployment_lock(self):
        """
        Serialize astronomy-shop Helm operations per worker cluster.

        Parallel workers run in isolated kind clusters, so using a single global
        lock causes unrelated workers to block each other unnecessarily.
        """
        cluster_name = os.getenv("SREGYM_KIND_CLUSTER_NAME") or f"pid-{os.getpid()}"
        safe_cluster_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in cluster_name)
        lock_path = f"/tmp/sregym-astronomy-shop-{safe_cluster_name}.lock"
        start = time.monotonic()
        lock_fd = open(lock_path, "w")
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            waited = time.monotonic() - start
            if waited > 1:
                logger.info(f"Astronomy shop deploy lock acquired after {waited:.1f}s: {lock_path}")
            yield
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()

    def create_workload(self):
        self.wrk = LocustWorkloadManager(
            namespace=self.namespace,
            locust_url="load-generator:8089",
        )

    def start_workload(self):
        if not hasattr(self, "wrk"):
            self.create_workload()
        self.wrk.start()

    def stop_workload(self):
        if hasattr(self, "wrk"):
            self.wrk.stop()


# Run this code to test installation/deletion
# if __name__ == "__main__":
#     shop = AstronomyShop()
#     shop.deploy()
#     shop.delete()
