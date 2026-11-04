"""Hot keys in a memcached instance hold poisoned values with a long TTL.

The consumer trusts its cache: a corrupt value fails its decoder (500s), a wrong-typed one fails
its typed reads, a stale one serves wrong data. The poison outlives consumer restarts. Reference:
delete the poisoned keys; flushing or restarting that memcached is also correct (it is a cache).
Key scopes: ``one_key`` / ``all_hot_keys`` poison the keys on the traffic probe's paths; ``broad`` also
poisons the instance's other live keys (lru_crawler metadump), so any user the traffic touches is hit.
Decoys: restore from source (no object changed) and rollout-restart the consuming service (the
poison is in memcached, not in it).
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Expect, Repair
from sregym.conductor.problems.composition.app_bindings import MemcachedInstance
from sregym.conductor.problems.composition.grading import Request
from sregym.generators.fault.components import http, memcached
from sregym.generators.fault.components.kube import ComponentError

POISON_TTL_S = 7 * 24 * 3600  # long enough to outlast any episode; memcached caps relative TTLs at 30 days
BROAD_KEY_LIMIT = 1000  # "broad" poisons up to this many of the instance's live keys
KEY_SCOPES = ("one_key", "all_hot_keys", "broad")


class MemcachedPoisonedEntries(ComponentFaultProblem):
    STATE_ATTRS = ("poisoned",)

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
        if corruption not in memcached.CORRUPTION_MODES:
            raise ValueError(f"unknown corruption {corruption!r}")
        if key_scope not in KEY_SCOPES:
            raise ValueError(f"unknown key_scope {key_scope!r}")
        self.corruption = corruption
        self.key_scope = key_scope
        self.poison_seed = poison_seed
        self.poisoned: dict[str, bytes] = {}
        super().__init__(
            app_name=app_name,
            target=target,
            description=(
                f"Hot cache entries in {target} were overwritten with {corruption.replace('_', ' ')} values and a "
                "long TTL. Its consumers trust the cache, so requests that read those entries fail or return "
                "wrong data until the entries are removed."
            ),
            namespace=namespace,
            kube=kube,
        )
        if not isinstance(self.target, MemcachedInstance):
            raise ValueError(f"{target} is not a memcached row of the {app_name} binding")
        self.instance: MemcachedInstance = self.target

    def hot_keys(self) -> tuple[str, ...]:
        """Literal keys, then ``key_field`` values from the snapshot and from the traffic probe's own
        reads (so the poison is on data the probe sees too, not only on seeded data)."""
        keys = list(self.instance.keys)
        if self.instance.key_field:
            values: list = list(self.snapshot.values) if self.snapshot is not None else []
            probe_reads = tuple(Request(p) for p in self.binding.entry.paths)
            values += [http.normalize(r.body) for r in http.fetch(self.kube, self.binding, self.namespace, probe_reads)]
            for value in http.json_field_values(values, self.instance.key_field):
                key = self.instance.key_template.format(value)
                if key not in keys:
                    keys.append(key)
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
        if self.key_scope == "broad":
            others = [k for k in memcached.live_keys(self.kube, self.namespace, inst, BROAD_KEY_LIMIT) if k not in cached]
            chosen = (cached + others)[:BROAD_KEY_LIMIT]
            current = {**current, **memcached.get_many(self.kube, self.namespace, inst, tuple(others))}
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

    def extra_repairs(self) -> dict[str, Repair]:
        inst = self.instance
        return {"flush_all": Repair(lambda: memcached.flush_all(self.kube, self.namespace, inst), Expect.PASSES)}
