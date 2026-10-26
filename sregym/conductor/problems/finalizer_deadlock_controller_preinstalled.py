"""Finalizer deadlock with an application-owned, source-installed controller."""

import time
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.problems.finalizer_deadlock_controller import (
    _CONTROLLER_SCRIPT_CONFIGMAP,
    FinalizerDeadlockController,
    _correct_clusterrole_rules,
)
from sregym.utils.decorators import mark_fault_injected


class FinalizerDeadlockControllerPreinstalled(FinalizerDeadlockController):
    """Fault only the source controller's RBAC; never simulate its reconciliation."""

    def __init__(self):
        # Removing write verbs produces real API 403s. ConfigMaps do not expose a
        # finalizers subresource, so the legacy simulated subresource mode is excluded.
        super().__init__(rbac_break_mode="remove_write_verbs")
        self._injected_at = None

    def _assert_preinstalled(self) -> None:
        dep = self.kubectl.apps_v1_api.read_namespaced_deployment(
            self.controller_name, self.namespace, _request_timeout=10
        )
        sa = self.kubectl.core_v1_api.read_namespaced_service_account(self.sa_name, self.namespace, _request_timeout=10)
        script = self.kubectl.core_v1_api.read_namespaced_config_map(
            _CONTROLLER_SCRIPT_CONFIGMAP, self.namespace, _request_timeout=10
        )
        role = self.rbac_v1.read_cluster_role(self.clusterrole_name, _request_timeout=10)
        binding = self.rbac_v1.read_cluster_role_binding(self.clusterrolebinding_name, _request_timeout=10)
        if any((obj.metadata.labels or {}).get("platform.io/injected") for obj in (dep, sa, script, role, binding)):
            raise RuntimeError("cleanup controller must be installed from application source")
        if (dep.spec.replicas or 0) < 1 or (dep.status.ready_replicas or 0) < dep.spec.replicas:
            raise RuntimeError("source-installed cleanup controller is not Ready")
        if dep.spec.template.spec.service_account_name != self.sa_name:
            raise RuntimeError("cleanup controller uses the wrong ServiceAccount")
        if not script.data or not script.data.get("controller.py"):
            raise RuntimeError("source-installed cleanup controller script is missing")
        expected = [r.to_dict() for r in _correct_clusterrole_rules()]
        if [r.to_dict() for r in role.rules or []] != expected:
            raise RuntimeError("source-installed cleanup controller RBAC is not correct")
        if (
            binding.role_ref.kind != "ClusterRole"
            or binding.role_ref.name != self.clusterrole_name
            or not any(
                s.kind == "ServiceAccount" and s.name == self.sa_name and s.namespace == self.namespace
                for s in binding.subjects or []
            )
        ):
            raise RuntimeError("source-installed cleanup controller binding is incorrect")
        try:
            self.kubectl.core_v1_api.read_namespaced_config_map(
                self.configmap_name, self.namespace, _request_timeout=10
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            raise RuntimeError("cleanup token already exists; recover the preceding fault first")

    @mark_fault_injected
    def inject_fault(self) -> None:
        self._assert_preinstalled()
        self._injected_at = datetime.now(timezone.utc)
        self.kubectl.core_v1_api.create_namespaced_config_map(
            namespace=self.namespace,
            body=client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name=self.configmap_name,
                    namespace=self.namespace,
                    finalizers=[self.finalizer],
                    labels={"platform.io/injected": "true"},
                ),
                data={"cleanup-token": "pending", "source": "reservation-maintenance"},
            ),
            _request_timeout=10,
        )
        self._replace_clusterrole(self._broken_rules_fn())
        self.kubectl.core_v1_api.delete_namespaced_config_map(
            self.configmap_name,
            self.namespace,
            body=client.V1DeleteOptions(grace_period_seconds=0),
            _request_timeout=10,
        )
        self._wait_for_forbidden_log()

    def _replace_clusterrole(self, rules) -> None:
        # Preserve source-owned metadata and all non-rule fields.
        self.rbac_v1.patch_cluster_role(
            name=self.clusterrole_name,
            body={"rules": client.ApiClient().sanitize_for_serialization(rules)},
            _request_timeout=10,
        )

    def _wait_for_forbidden_log(self, timeout_seconds: int = 45) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            pods = self.kubectl.core_v1_api.list_namespaced_pod(
                namespace=self.namespace, label_selector=f"app={self.controller_name}", _request_timeout=10
            ).items
            for pod in pods:
                logs = self.kubectl.core_v1_api.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=self.namespace,
                    since_time=self._injected_at,
                    tail_lines=50,
                    _request_timeout=10,
                )
                if "Kubernetes API denied the finalizer cleanup request with Forbidden" in logs:
                    return
            time.sleep(1)
        raise TimeoutError("source-installed cleanup controller did not log a real Forbidden response")

    @mark_fault_injected
    def recover_fault(self) -> None:
        self._replace_clusterrole(_correct_clusterrole_rules())
        try:
            self.kubectl.core_v1_api.delete_namespaced_config_map(
                self.configmap_name,
                self.namespace,
                body=client.V1DeleteOptions(grace_period_seconds=0),
                _request_timeout=10,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        if not self._wait_until_configmap_deleted(timeout_seconds=45):
            # Recovery can remove runtime data even if the responder deleted the
            # controller. Never delete/recreate source objects or forge completion.
            self.kubectl.core_v1_api.patch_namespaced_config_map(
                self.configmap_name, self.namespace, {"metadata": {"finalizers": []}}, _request_timeout=10
            )
            if not self._wait_until_configmap_deleted(timeout_seconds=10):
                raise TimeoutError("runtime cleanup token remains after recovery")


def preinstalled_slot11_plan():
    """Keep slot 11's exact baseline draws, replacing only the finalizer fault."""
    from dataclasses import replace

    from sregym.conductor.problems.composition import catalog
    from sregym.conductor.problems.composition.generator import PlanRef, plan_for

    baseline = plan_for(PlanRef("hotel_reservation", 3, 1276337490, reliable=True), catalog.entries())
    draws = tuple(
        replace(draw, key="finalizer_deadlock_controller_preinstalled", params=())
        if draw.key == "finalizer_deadlock_controller"
        else draw
        for draw in baseline.draws
    )
    if sum(draw.key == "finalizer_deadlock_controller_preinstalled" for draw in draws) != 1:
        raise ValueError("slot 11 no longer contains exactly one finalizer fault")
    return replace(baseline, draws=draws)


def preinstalled_slot11():
    """Use the same colocated lifecycle/per-fault grading as the original slot."""
    from dataclasses import replace

    from sregym.conductor.problems.composition import build_composite, catalog

    entries = tuple(
        replace(
            entry,
            key="finalizer_deadlock_controller_preinstalled",
            build=lambda draw: FinalizerDeadlockControllerPreinstalled(),
        )
        if entry.key == "finalizer_deadlock_controller"
        else entry
        for entry in catalog.entries()
    )
    return build_composite(preinstalled_slot11_plan(), entries=entries)
