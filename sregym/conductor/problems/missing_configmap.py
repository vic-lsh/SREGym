import json
import os
import subprocess

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.paths import fault_scratch_path
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.apps.social_network import SocialNetwork
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_CONFIGMAP_FAULT_MODES = {
    "delete": {
        "description": (
            "A required ConfigMap for deployment `{faulty_service}` has been deleted, so pods lose required "
            "runtime configuration during startup and reload. Affected pods fail to initialize correctly or run "
            "with invalid defaults, leading to NotReady/CrashLoop behavior and unstable service operation. "
            "Users observe request failures and degraded functionality for features backed by this component."
        ),
    },
    "clear_data": {
        "description": (
            "ConfigMap for deployment `{faulty_service}` has had its data keys cleared to empty values, "
            "causing pods to receive blank configuration. Unlike a missing ConfigMap, no Kubernetes-level "
            "errors are generated -- the ConfigMap exists but all values are empty strings. Pods start "
            "with empty/invalid configuration, leading to silent misconfiguration, connection failures, "
            "or degraded functionality that requires inspecting ConfigMap contents to diagnose."
        ),
    },
}

# Namespace-to-ConfigMap name mapping, matching VirtualizationFaultInjector's
# hardcoded names in inject_missing_configmap().
_NAMESPACE_CONFIGMAP_NAMES = {
    "social-network": "media-mongodb",
    "hotel-reservation": "mongo-geo-script",
}


class MissingConfigMap(Problem):
    """Delete the ConfigMap one or more workloads mount, then restart them so new pods cannot start.

    ``faulty_service`` names one Deployment or a list of Deployments. The injector resolves each
    Deployment's required ConfigMap (``VirtualizationFaultInjector.REQUIRED_CONFIGMAPS``).
    """

    def __init__(
        self,
        app_name: str = "social_network",
        faulty_service: str | list[str] = "media-mongodb",
        configmap_fault_mode: str = "delete",
    ):
        services = [faulty_service] if isinstance(faulty_service, str) else list(faulty_service)
        if not services or not all(isinstance(service, str) and service for service in services):
            raise ValueError("faulty_service must name at least one deployment")
        if len(set(services)) != len(services):
            raise ValueError(f"faulty_service lists a deployment more than once: {services}")
        self.faulty_services = services
        self.faulty_service = ", ".join(services)
        self.app_name = app_name
        if configmap_fault_mode not in _CONFIGMAP_FAULT_MODES:
            raise ValueError(f"Unknown ConfigMap fault mode: {configmap_fault_mode}")
        if configmap_fault_mode != "delete" and len(services) != 1:
            raise ValueError("clear_data requires exactly one deployment")
        self.configmap_fault_mode = configmap_fault_mode
        self._original_configmap_data: dict[str, dict] = {}

        if app_name == "hotel_reservation":
            app = HotelReservation()
        elif app_name == "social_network":
            app = SocialNetwork()
        else:
            app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                self._describe(services)
                if configmap_fault_mode == "delete"
                else _CONFIGMAP_FAULT_MODES[configmap_fault_mode]["description"].format(faulty_service=services[0])
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    @staticmethod
    def _describe(services: list[str]) -> str:
        if len(services) == 1:
            subject = f"A required ConfigMap for deployment `{services[0]}` has been deleted"
            component = "this component"
        else:
            names = ", ".join(f"`{service}`" for service in services[:-1]) + f" and `{services[-1]}`"
            subject = f"The required ConfigMap of each of the deployments {names} has been deleted"
            component = "these components"
        return (
            f"{subject}, so pods lose required "
            "runtime configuration during startup and reload. Affected pods fail to initialize correctly or run "
            "with invalid defaults, leading to NotReady/CrashLoop behavior and unstable service operation. "
            f"Users observe request failures and degraded functionality for features backed by {component}."
        )

    def _get_configmap_name(self) -> str:
        for prefix, name in _NAMESPACE_CONFIGMAP_NAMES.items():
            if self.namespace.startswith(prefix):
                return name
        raise ValueError(f"No ConfigMap mapping for namespace {self.namespace}")

    def _backup_path(self) -> str:
        return fault_scratch_path(f"{self.namespace}_{self.faulty_services[0]}_configmap_backup.json")

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        if self.configmap_fault_mode == "delete":
            injector = VirtualizationFaultInjector(namespace=self.namespace)
            injector._inject(fault_type="missing_configmap", microservices=self.faulty_services)
        elif self.configmap_fault_mode == "clear_data":
            cm_name = self._get_configmap_name()
            print(f"Targeting ConfigMap {cm_name} for {self.faulty_service}")

            # Save original data
            result = subprocess.run(
                ["kubectl", "get", "configmap", cm_name, "-n", self.namespace, "-o", "json"],
                capture_output=True,
                text=True,
                check=True,
            )
            cm_obj = json.loads(result.stdout)
            original_data = cm_obj.get("data", {})
            self._original_configmap_data[cm_name] = original_data

            # Persist to disk for crash-safe recovery
            with open(self._backup_path(), "w") as f:
                json.dump(self._original_configmap_data, f)

            if not original_data:
                raise RuntimeError(f"ConfigMap {cm_name} in namespace {self.namespace} has no data keys to clear")

            # Patch all data values to empty strings
            cleared_data = {k: "" for k in original_data}
            patch = json.dumps({"data": cleared_data})
            subprocess.run(
                ["kubectl", "patch", "configmap", cm_name, "-n", self.namespace, "--type", "merge", "-p", patch],
                check=True,
            )
            print(f"Cleared data in ConfigMap {cm_name}")

            # Restart pods to pick up the cleared config
            subprocess.run(
                ["kubectl", "rollout", "restart", f"deployment/{self.faulty_service}", "-n", self.namespace],
                check=True,
            )

        print(f"Service: {self.faulty_service} | Namespace: {self.namespace} | Mode: {self.configmap_fault_mode}")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        if self.configmap_fault_mode == "delete":
            injector = VirtualizationFaultInjector(namespace=self.namespace)
            injector._recover(fault_type="missing_configmap", microservices=self.faulty_services)
        else:
            if not self._original_configmap_data:
                with open(self._backup_path()) as backup:
                    self._original_configmap_data = json.load(backup)
            for cm_name, original_data in self._original_configmap_data.items():
                subprocess.run(
                    [
                        "kubectl",
                        "patch",
                        "configmap",
                        cm_name,
                        "-n",
                        self.namespace,
                        "--type",
                        "merge",
                        "-p",
                        json.dumps({"data": original_data}),
                    ],
                    check=True,
                )
            subprocess.run(
                ["kubectl", "rollout", "restart", f"deployment/{self.faulty_services[0]}", "-n", self.namespace],
                check=True,
            )
            if os.path.exists(self._backup_path()):
                os.remove(self._backup_path())
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")
