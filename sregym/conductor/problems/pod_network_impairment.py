"""netem delay or packet loss on a workload's pod interface (egress), applied from the node.

Reference: remove the qdisc. Replacing the pod also clears it, so a restart is acceptable for
stateless services and caches and a decoy for workloads holding the only copy of user data.
"""

from __future__ import annotations

from sregym.conductor.problems.pod_fault import PodLevelFault
from sregym.generators.fault.components import network


class PodNetworkImpairment(PodLevelFault):
    def __init__(
        self, app_name: str, target: str, mode: str, severity: int, namespace: str | None = None, kube=None, nodes=None
    ):
        network.netem_args(mode, severity)  # validates
        self.mode, self.severity = mode, severity
        unit = "ms of added delay" if mode == "delay" else "% packet loss"
        super().__init__(
            app_name,
            target,
            nodes=nodes,
            namespace=namespace,
            kube=kube,
            component_detail=(
                f"The network interface of the {target} pod has a traffic-control (netem) qdisc adding {severity}{unit} "
                "to everything it sends. Requests through it are slow or time out, while the pod looks healthy."
            ),
        )

    def _args(self):
        t = self.pod
        return self.namespace, t.kind, t.workload, t.container

    def inject(self) -> None:
        network.impair(self.nodes, *self._args(), self.mode, self.severity)

    def fault_cleared(self) -> tuple[bool, str]:
        impaired, detail = network.is_impaired(self.nodes, *self._args())
        return not impaired, detail

    def reference_mitigation(self) -> None:
        network.clear(self.nodes, *self._args())
