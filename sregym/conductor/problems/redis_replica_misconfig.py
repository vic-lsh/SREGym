"""A Redis-protocol server is turned into a read-only replica of a master that does not exist.

Writes through it fail with READONLY while reads keep working, so the app's write paths break
and nothing restarts. Reference: ``REPLICAOF NO ONE``. Decoys: restore from source (no object
changed: no-op) and, when the server holds the only copy of its data, rollout-restarting it
(clears the replica role but loses the data, which the seeded-data check sees).
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Mitigation
from sregym.conductor.problems.composition.app_bindings import BINDINGS
from sregym.generators.fault.components import redis
from sregym.generators.fault.components.kube import rollout_restart


class RedisReplicaMisconfig(ComponentFaultProblem):
    def __init__(self, app_name: str, target: str, master: str, namespace: str | None = None, kube=None):
        matches = [r for r in BINDINGS[app_name].redis if r.workload == target]
        if not matches:
            raise ValueError(f"{app_name} binding has no redis instance {target!r}")
        self.instance = matches[0]
        self.master_host, port = master.rsplit(":", 1)
        self.master_port = int(port)
        super().__init__(
            app_name=app_name,
            component=f"deployment/{target}",
            description=(
                f"The Redis server {target} was reconfigured at runtime as a replica of {master}, "
                "a master that does not exist, so it rejects every write with READONLY while still serving "
                "stale reads. Requests that write through it fail."
            ),
            namespace=namespace,
            kube=kube,
        )

    def inject(self) -> None:
        redis.make_replica_of(self.kube, self.namespace, self.instance, self.master_host, self.master_port)

    def fault_cleared(self) -> tuple[bool, str]:
        info = redis.replication_info(self.kube, self.namespace, self.instance)
        role = info.get("role", "?")
        return role == "master", f"{self.instance.workload} role={role} master_host={info.get('master_host', '-')}"

    def reference_mitigation(self) -> None:
        redis.promote(self.kube, self.namespace, self.instance)

    def _restart(self) -> None:
        rollout_restart(self.kube, self.namespace, self.instance.kind, self.instance.workload)

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        if self.instance.holds_primary_data:  # restart clears the role but loses the only copy
            decoys["rollout_restart"] = self._restart
        return decoys

    def acceptable_mitigations(self) -> dict[str, Mitigation]:
        # A cache over a database: a restart comes back as an empty master and refills.
        return {} if self.instance.holds_primary_data else {"rollout_restart": self._restart}
