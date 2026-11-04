"""A Redis-protocol server is turned into a read-only replica of a master that does not exist.

Writes through it fail with READONLY while reads keep working, so the app's write paths break
and nothing restarts. Reference: ``REPLICAOF NO ONE``. Other repairs are the standard ones from
the target row: restarting it clears the role and is acceptable for a cache, but loses the data
(DATA_LOST, seen by the store witness) when the server holds the only copy.
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem
from sregym.conductor.problems.composition.app_bindings import RedisInstance
from sregym.generators.fault.components import redis


class RedisReplicaMisconfig(ComponentFaultProblem):
    blocks_writes = True  # READONLY: the verdict also needs a write through it to land

    def __init__(self, app_name: str, target: str, master: str, namespace: str | None = None, kube=None):
        self.master_host, port = master.rsplit(":", 1)
        self.master_port = int(port)
        super().__init__(
            app_name=app_name,
            target=target,
            description=(
                f"The Redis server {target} was reconfigured at runtime as a replica of {master}, "
                "a master that does not exist, so it rejects every write with READONLY while still serving "
                "stale reads. Requests that write through it fail."
            ),
            namespace=namespace,
            kube=kube,
        )
        if not isinstance(self.target, RedisInstance):
            raise ValueError(f"{target} is not a redis row of the {app_name} binding")
        self.instance: RedisInstance = self.target

    def inject(self) -> None:
        redis.make_replica_of(self.kube, self.namespace, self.instance, self.master_host, self.master_port)

    def fault_cleared(self) -> tuple[bool, str]:
        info = redis.replication_info(self.kube, self.namespace, self.instance)
        role = info.get("role", "?")
        return role == "master", f"{self.instance.workload} role={role} master_host={info.get('master_host', '-')}"

    def reference_mitigation(self) -> None:
        redis.promote(self.kube, self.namespace, self.instance)
