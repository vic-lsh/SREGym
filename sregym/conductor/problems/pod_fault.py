"""Base for faults on one bound workload's pod, injected from the node (freeze, netem).

``self.target`` is the app binding's row for the workload (the standard repairs come from it): where the
workload holds the only copy of user data (a primary-data Redis, a Mongo) a restart clears the fault by
destroying data (DATA_LOST, a decoy caught by the seeded-data check and the store witness); where it is
stateless or a rebuildable cache a restart is a legitimate repair (PASSES), because the fault lives in
the process or the pod sandbox that a restart replaces. ``self.pod`` is the same workload as a
``WorkloadTarget`` (kind, container, port) for the drivers.
"""

from __future__ import annotations

import time

from sregym.conductor.problems.component_fault import ComponentFaultProblem
from sregym.conductor.problems.composition.app_bindings import BINDINGS, WorkloadTarget, workload_targets
from sregym.conductor.problems.composition.grading import Request
from sregym.generators.fault.components import http
from sregym.generators.fault.components.kube import ComponentError
from sregym.generators.fault.components.node import Nodes


def pod_target(app_name: str, workload: str) -> WorkloadTarget:
    rows = [t for t in workload_targets(BINDINGS[app_name], verified_only=False) if t.workload == workload]
    if not rows:
        raise ValueError(f"{app_name} binding has no workload {workload!r}")
    return rows[0]


class PodLevelFault(ComponentFaultProblem):
    """A fault in one pod's process or network namespace; a restart replaces the pod and clears it."""

    restart_clears = True

    def __init__(self, app_name: str, target: str, *, component_detail: str, nodes: Nodes | None = None, **kw):
        self.pod = pod_target(app_name, target)
        super().__init__(app_name=app_name, target=target, description=component_detail, **kw)
        self.nodes = nodes or Nodes(self.kube)


class ForeignPodFault(ComponentFaultProblem):
    """A fault whose cause is a pod outside the app namespace (a hog, a surge, a noisy neighbor).

    The fault is cleared when that pod is gone. Restarting or restoring the app's own objects does
    not end it (the cause keeps running and reconnects), so the standard repairs are decoys
    (``restart_clears`` False); the reference stops the injector pod. ``victim`` is the workload the
    injector pod is named after and whose restart is the tempting repair.
    """

    FAULT_KIND = ""
    restart_clears = False

    def __init__(self, app_name: str, victim: str, *, description: str, **kw):
        self.pod = pod_target(app_name, victim)
        super().__init__(app_name=app_name, target=victim, description=description, **kw)

    def fault_cleared(self) -> tuple[bool, str]:
        from sregym.generators.fault.components import foreign

        present = foreign.exists(self.kube, self.FAULT_KIND, self.namespace, self.pod.workload)
        return not present, f"{self.FAULT_KIND} pod for {self.pod.workload} {'still running' if present else 'gone'}"

    def reference_mitigation(self) -> None:
        from sregym.generators.fault.components import foreign

        foreign.stop(self.kube, self.FAULT_KIND, self.namespace, self.pod.workload)
        self._wait_drained()

    def _wait_drained(self, timeout_s: float = 240.0) -> None:
        """Stopping the cause is not the end of the repair: the queued requests and dropped connections it
        left drain over a minute or two. Wait (bounded) until the entry point's reads succeed again."""
        entry = self.binding.entry
        reads = tuple(Request(p) for p in entry.paths) + tuple(
            Request(p) for p in (self.binding.integrity.reads if self.binding.integrity else ())
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                if all(r.ok for r in http.fetch(self.kube, self.binding, self.namespace, reads)):
                    return
            except ComponentError:
                pass
            time.sleep(5)
