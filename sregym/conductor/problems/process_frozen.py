"""A workload's main process is frozen (SIGSTOP from the node): the pod stays Running and Ready, accepts
connections, and never answers.

Reference: thaw it (``kill -CONT 1`` in the pod). Restarting a stateless service or a cache also
clears it (acceptable); restarting a workload that holds the only copy of user data clears the
stop by destroying the data (decoy, caught by the seeded-data check). Restore-from-source is a
decoy everywhere: no Kubernetes object is wrong.
"""

from __future__ import annotations

from sregym.conductor.problems.pod_fault import PodLevelFault
from sregym.generators.fault.components import process


class ProcessFrozen(PodLevelFault):
    def __init__(self, app_name: str, target: str, namespace: str | None = None, kube=None, nodes=None):
        super().__init__(
            app_name,
            target,
            nodes=nodes,
            namespace=namespace,
            kube=kube,
            component_detail=(
                f"The main process of {target} was stopped (SIGSTOP) on its node. The pod is Running and Ready and "
                "its port accepts connections, but the process never answers, so requests that need it hang until "
                "they time out."
            ),
        )

    def _args(self):
        t = self.target
        return self.namespace, t.kind, t.workload, t.container

    def inject(self) -> None:
        process.freeze(self.nodes, *self._args())

    def fault_cleared(self) -> tuple[bool, str]:
        frozen, detail = process.is_frozen(self.nodes, *self._args())
        return not frozen, detail

    def reference_mitigation(self) -> None:
        process.thaw(self.kube, *self._args())
