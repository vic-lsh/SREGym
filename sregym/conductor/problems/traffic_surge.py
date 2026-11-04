"""A client outside the app namespace floods the app's entry point with requests.

The services have fixed small CPU budgets, so a surge saturates them: requests queue, time out, or
fail. Reference: stop the surge at its source (the repair a rate limit at the edge would also
achieve; removing the source needs no change to the app's accepted configuration, whereas scaling
out is a drift from accepted source that a restore would revert). Decoys: restore from source and
rollout-restart the entry proxy.
"""

from __future__ import annotations

from sregym.conductor.problems.pod_fault import ForeignPodFault
from sregym.generators.fault.components import load


class TrafficSurge(ForeignPodFault):
    FAULT_KIND = load.SURGE

    def __init__(self, app_name: str, target: str, workers: int, namespace: str | None = None, kube=None):
        if workers <= 0:
            raise ValueError("workers must be positive")
        self.workers = workers
        super().__init__(
            app_name,
            target,
            namespace=namespace,
            kube=kube,
            description=(
                f"A client outside the application namespace sends a sustained flood of requests ({workers} concurrent "
                f"loops) through {target}, saturating the services behind it so requests queue, time out or fail."
            ),
        )

    def inject(self) -> None:
        entry = self.binding.entry
        base = f"http://{entry.service}.{self.namespace}.svc.cluster.local:{entry.port}"
        reqs = tuple(load.Request(p) for p in entry.paths[1:]) + tuple(entry.writes[:1])
        load.start_surge(self.kube, self.namespace, self.pod.workload, base, reqs, self.workers, cpu_limit="2")
