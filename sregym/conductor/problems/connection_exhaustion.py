"""A client outside the app namespace holds thousands of idle connections open against one service.

The service is a thread-per-connection server: each held connection costs a thread and memory, so
the pod runs out of its memory budget (stalls, then OOM kills and restarts) while staying
Running. The client re-opens its connections after every restart, so restarting the victim does
not help. Reference: stop the client. Decoys: restore from source, rollout-restart the victim.
"""

from __future__ import annotations

from sregym.conductor.problems.pod_fault import ForeignPodFault
from sregym.generators.fault.components import load


class ConnectionExhaustion(ForeignPodFault):
    FAULT_KIND = load.HOG

    def __init__(self, app_name: str, target: str, connections: int, namespace: str | None = None, kube=None):
        if connections <= 0:
            raise ValueError("connections must be positive")
        self.connections = connections
        super().__init__(
            app_name,
            target,
            namespace=namespace,
            kube=kube,
            description=(
                f"A client pod outside the application namespace holds {connections} idle connections open to {target}, "
                "exhausting its threads and memory so it stalls or restarts; the client reconnects after every restart."
            ),
        )

    def inject(self) -> None:
        t = self.target
        host = f"{t.workload}.{self.namespace}.svc.cluster.local"
        load.start_hog(self.kube, self.namespace, t.kind, t.workload, t.container, host, t.port, self.connections)
