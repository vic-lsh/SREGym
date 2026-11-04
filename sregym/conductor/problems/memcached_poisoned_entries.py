"""Hot keys in a memcached instance hold poisoned values with a long TTL.

The consumer trusts its cache: a corrupt value fails its decoder (500s), a wrong-typed one fails
its typed reads, a stale one serves wrong data. The poison outlives consumer restarts. Reference:
delete the poisoned keys; flushing or restarting that memcached is also correct (it is a cache).
Decoys: restore from source (no object changed) and rollout-restart the consuming service (the
poison is in memcached, not in it).
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Mitigation
from sregym.conductor.problems.composition.app_bindings import BINDINGS
from sregym.generators.fault.components import http, memcached
from sregym.generators.fault.components.kube import ComponentError, delete_pods, rollout_restart

POISON_TTL_S = 7 * 24 * 3600  # long enough to outlast any episode; memcached caps relative TTLs at 30 days


class MemcachedPoisonedEntries(ComponentFaultProblem):
    def __init__(
        self,
        app_name: str,
        target: str,
        corruption: str,
        key_scope: str,
        poison_seed: int,
        namespace: str | None = None,
        kube=None,
    ):
        matches = [m for m in BINDINGS[app_name].memcached if m.workload == target]
        if not matches:
            raise ValueError(f"{app_name} binding has no memcached instance {target!r}")
        if corruption not in memcached.CORRUPTION_MODES:
            raise ValueError(f"unknown corruption {corruption!r}")
        if key_scope not in ("one_key", "all_hot_keys"):
            raise ValueError(f"unknown key_scope {key_scope!r}")
        self.instance = matches[0]
        self.corruption = corruption
        self.key_scope = key_scope
        self.poison_seed = poison_seed
        self.poisoned: dict[str, bytes] = {}
        super().__init__(
            app_name=app_name,
            component=f"deployment/{target}",
            description=(
                f"Hot cache entries in {target} were overwritten with {corruption.replace('_', ' ')} values and a "
                f"long TTL. Its consumers ({', '.join(self.instance.consumers)}) trust the cache, so requests "
                "that read those entries fail or return wrong data until the entries are removed."
            ),
            namespace=namespace,
            kube=kube,
        )

    def hot_keys(self) -> tuple[str, ...]:
        keys = list(self.instance.keys)
        if self.instance.key_field and self.snapshot is not None:
            keys += [k for k in self.snapshot.json_field_values(self.instance.key_field) if k not in keys]
        return tuple(keys)

    def inject(self) -> None:
        inst = self.instance
        if inst.warm:
            http.fetch(self.kube, self.binding, self.namespace, inst.warm)
        keys = self.hot_keys()
        current = memcached.get_many(self.kube, self.namespace, inst, keys)
        cached = [k for k in keys if current[k] is not None]
        if not cached:
            raise ComponentError(f"none of the hot keys {list(keys)} is cached in {inst.workload}")
        chosen = cached[:1] if self.key_scope == "one_key" else cached
        self.poisoned = {
            k: memcached.corrupt(current[k] or b"", self.corruption, self.poison_seed + i) for i, k in enumerate(chosen)
        }
        memcached.set_many(self.kube, self.namespace, inst, self.poisoned, POISON_TTL_S)

    def fault_cleared(self) -> tuple[bool, str]:
        if not self.poisoned:
            return False, "nothing poisoned (fault not injected here)"
        now = memcached.get_many(self.kube, self.namespace, self.instance, tuple(self.poisoned))
        still = [k for k, v in self.poisoned.items() if now[k] == v]
        return not still, f"{len(still)}/{len(self.poisoned)} poisoned keys still cached in {self.instance.workload}"

    def reference_mitigation(self) -> None:
        memcached.delete_many(self.kube, self.namespace, self.instance, tuple(self.poisoned))

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        for svc in self.instance.consumers:
            decoys[f"rollout_restart:{svc}"] = lambda svc=svc: rollout_restart(
                self.kube, self.namespace, "Deployment", svc
            )
        return decoys

    def acceptable_mitigations(self) -> dict[str, Mitigation]:
        inst = self.instance
        return {
            "flush_all": lambda: memcached.flush_all(self.kube, self.namespace, inst),
            # A cache: restarting memcached empties it and consumers refill from the database.
            "restart_stateful_pod": lambda: delete_pods(self.kube, self.namespace, inst.kind, inst.workload),
        }
