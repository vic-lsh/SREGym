"""Base for faults on one bound workload's pod, injected from the node (freeze, netem).

The target is a ``WorkloadTarget`` row of the app binding. Restart repairs are per target:
where the workload holds the only copy of user data (a primary-data Redis, a Mongo) a restart
clears the fault by destroying data, so the seeded-data check fails it (decoy); where the workload is
stateless or a rebuildable cache a restart is a legitimate repair (acceptable), because the fault
lives in the process or the pod sandbox that a restart replaces.
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Mitigation
from sregym.conductor.problems.composition.app_bindings import BINDINGS, WorkloadTarget, workload_targets
from sregym.generators.fault.components.kube import delete_pods, rollout_restart
from sregym.generators.fault.components.node import Nodes


class PodLevelFault(ComponentFaultProblem):
    def __init__(self, app_name: str, target: str, *, component_detail: str, nodes: Nodes | None = None, **kw):
        rows = [t for t in workload_targets(BINDINGS[app_name], verified_only=False) if t.workload == target]
        if not rows:
            raise ValueError(f"{app_name} binding has no workload {target!r}")
        self.target: WorkloadTarget = rows[0]
        self.nodes_override = nodes
        super().__init__(app_name=app_name, component=f"deployment/{target}", description=component_detail, **kw)
        self.nodes = nodes or Nodes(self.kube)

    def _restart(self) -> None:
        rollout_restart(self.kube, self.namespace, self.target.kind, self.target.workload)

    def _delete_pod(self) -> None:
        delete_pods(self.kube, self.namespace, self.target.kind, self.target.workload)

    def restart_repairs(self) -> dict[str, Mitigation]:
        return {"rollout_restart": self._restart, "restart_stateful_pod": self._delete_pod}

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        if self.target.holds_primary_data:
            decoys.update(self.restart_repairs())
        return decoys

    def acceptable_mitigations(self) -> dict[str, Mitigation]:
        return {} if self.target.holds_primary_data else self.restart_repairs()


class ForeignPodFault(ComponentFaultProblem):
    """A fault whose cause is a pod outside the app namespace (a hog, a surge, a noisy neighbor).

    The fault is cleared when that pod is gone. Restarting or restoring the app's own objects does
    not end it (the cause keeps running), so those are decoys; the reference stops the injector pod.
    ``victim`` is the workload the injector pod is named after and whose restart is the tempting repair.
    """

    FAULT_KIND = ""

    def __init__(self, app_name: str, victim: str, *, description: str, **kw):
        rows = [t for t in workload_targets(BINDINGS[app_name], verified_only=False) if t.workload == victim]
        if not rows:
            raise ValueError(f"{app_name} binding has no workload {victim!r}")
        self.target: WorkloadTarget = rows[0]
        super().__init__(app_name=app_name, component=f"deployment/{victim}", description=description, **kw)

    def fault_cleared(self) -> tuple[bool, str]:
        from sregym.generators.fault.components import foreign

        present = foreign.exists(self.kube, self.FAULT_KIND, self.namespace, self.target.workload)
        return not present, f"{self.FAULT_KIND} pod for {self.target.workload} {'still running' if present else 'gone'}"

    def reference_mitigation(self) -> None:
        from sregym.generators.fault.components import foreign

        foreign.stop(self.kube, self.FAULT_KIND, self.namespace, self.target.workload)

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        decoys["rollout_restart"] = lambda: rollout_restart(
            self.kube, self.namespace, self.target.kind, self.target.workload
        )
        return decoys
