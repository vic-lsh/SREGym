"""A CPU-burning pod in another namespace is scheduled on the node that runs an app workload.

Co-located pods get a sliver of CPU, so the app slows or times out although its own manifests and
pods are fine. Reference: stop the neighbor (evict or delete it). Decoys: restore from source,
rollout-restart the victim (it comes back on the same node).

NOT in the catalog: on the one-worker kind lane (docker-capped to 3 CPUs) no burner calibration made the
symptom visible. Six busy loops with a 6-CPU request and sixteen with a 30-CPU request both left the
entry-point probe at 36/36 (it only slowed from 8 s to 14 s), because the services are idle between
requests and the kernel still gives them their short bursts. Making it visible needs a latency budget
in the verdict, which would be a weaker check than the other faults carry.
"""

from __future__ import annotations

from sregym.conductor.problems.pod_fault import ForeignPodFault
from sregym.generators.fault.components import neighbor


class NoisyNeighbor(ForeignPodFault):
    FAULT_KIND = neighbor.NEIGHBOR

    def __init__(
        self, app_name: str, target: str, workers: int, cpu_request: str, namespace: str | None = None, kube=None
    ):
        if workers <= 0:
            raise ValueError("workers must be positive")
        self.workers, self.cpu_request = workers, cpu_request
        super().__init__(
            app_name,
            target,
            namespace=namespace,
            kube=kube,
            description=(
                f"A CPU-burning pod ({workers} busy loops, CPU request {cpu_request}) in another namespace is running on "
                f"the same node as {target} and starves the pods there of CPU."
            ),
        )

    def inject(self) -> None:
        neighbor.start(
            self.kube,
            self.namespace,
            self.pod.kind,
            self.pod.workload,
            workers=self.workers,
            cpu_request=self.cpu_request,
            cpu_limit=self.cpu_request,
        )
