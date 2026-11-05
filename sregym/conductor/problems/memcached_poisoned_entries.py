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

import json

from sregym.conductor.problems import scope as scopes
from sregym.conductor.problems.component_fault import ComponentFaultProblem, Expect, Repair
from sregym.conductor.problems.composition.app_bindings import MemcachedInstance, RedisInstance
from sregym.conductor.problems.composition.grading import Request
from sregym.generators.fault.components import http, memcached, redis
from sregym.generators.fault.components.kube import ComponentError

POISON_TTL_S = 7 * 24 * 3600  # long enough to outlast any episode; memcached caps relative TTLs at 30 days
BROAD_KEY_LIMIT = 1000  # "broad" poisons up to this many of the instance's live keys
KEY_SCOPES = ("one_key", "all_hot_keys", "broad")
# Catalog generation 3 (problems/scope.py): ``wide`` is ``broad`` restricted to seeded users' entries, ``partial`` a
# seeded fraction of them (``single`` is ``one_key``). Entries are chosen from what the cache really holds after seeded
# users' reads, never the probe's keys.
KEY_SCOPES_G3 = ("single", "partial", "wide")
WARM_USERS = 40  # seeded users whose timelines are read to fill a cold cache


class MemcachedPoisonedEntries(ComponentFaultProblem):
    STATE_ATTRS = ("poisoned",)

    def __init__(
        self,
        app_name: str,
        target: str,
        corruption: str,
        key_scope: str,
        poison_seed: int,
        fraction: float | None = None,
        pick_seed: int = 0,
        namespace: str | None = None,
        kube=None,
    ):
        if corruption not in memcached.CORRUPTION_MODES:
            raise ValueError(f"unknown corruption {corruption!r}")
        if key_scope not in (*KEY_SCOPES, *KEY_SCOPES_G3):
            raise ValueError(f"unknown key_scope {key_scope!r}")
        scopes.validate("single" if key_scope in KEY_SCOPES else key_scope, fraction)
        self.corruption = corruption
        self.fraction, self.pick_seed = fraction, pick_seed
        self.key_scope = "one_key" if key_scope == "single" else key_scope
        self.scoped = self.key_scope in ("partial", "wide")
        self.scoped_keys: dict[str, str] = {}  # cache key -> the user it belongs to, chosen while healthy
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
        if self.scoped and not (self.instance.reader_field and self.instance.read):
            raise ValueError(f"{target} has no reader_field/read in its binding: no {self.key_scope} scope")

    def prepare_target(self) -> None:
        """Scoped: fill a cold cache by reading as seeded users, then choose the entries from what it holds."""
        if not self.scoped:
            return
        inst = self.instance
        assert inst.reader_field and inst.read
        users = self._seeded_users()
        http.fetch(self.kube, self.binding, self.namespace, tuple(Request(inst.read.format(uid=u)) for u in users))
        live = memcached.live_keys(self.kube, self.namespace, inst, BROAD_KEY_LIMIT)
        values = memcached.get_many(self.kube, self.namespace, inst, tuple(live))
        eligible: dict[str, str] = {}
        for key, value in values.items():
            uid = _reader(value, inst.reader_field)
            if uid is not None and uid.isdigit() and int(uid) < inst.uid_below:
                eligible[key] = uid
        if len(eligible) < inst.min_keys:
            raise ComponentError(f"{inst.workload}: {len(eligible)} cached seeded entries, need {inst.min_keys}")
        chosen = scopes.select(sorted(eligible), self.key_scope, self.fraction, self.pick_seed)
        self.scoped_keys = {k: eligible[k] for k in chosen}

    def _seeded_users(self) -> tuple[str, ...]:
        inst = self.instance
        source = self.binding.row(inst.warm_from) if inst.warm_from else None
        if not isinstance(source, RedisInstance):
            raise ComponentError(f"{inst.workload}: warm_from must name a redis row of user-id keys")
        keys = redis.scan_keys(self.kube, self.namespace, source, "[0-9]*")
        users = [k for k in keys if k.isdigit() and int(k) < inst.uid_below][:WARM_USERS]
        if not users:
            raise ComponentError(f"{source.workload} holds no seeded user keys to warm {inst.workload} from")
        return tuple(users)

    def extra_reads(self) -> tuple[str, ...]:
        if not self.scoped or not self.instance.read:
            return ()
        users = sorted(set(self.scoped_keys.values()), key=int)
        return tuple(self.instance.read.format(uid=u) for u in scopes.spread(tuple(users), 3))
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
        if self.scoped:
            if not self.scoped_keys:
                raise ComponentError("scoped keys were not chosen (prepare_injection did not run)")
            current = memcached.get_many(self.kube, self.namespace, inst, tuple(self.scoped_keys))
            self.poisoned = {
                k: memcached.corrupt(current[k] or b"", self.corruption, self.poison_seed + i)
                for i, k in enumerate(self.scoped_keys)
            }
            memcached.set_many(self.kube, self.namespace, inst, self.poisoned, POISON_TTL_S)
            return
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
        repairs = {"flush_all": Repair(lambda: memcached.flush_all(self.kube, self.namespace, inst), Expect.PASSES)}
        if self.scoped:
            first = tuple(self.scoped_keys)[:1]
            # one entry removed, the rest of the incident left
            repairs["delete_first_key"] = Repair(
                lambda: memcached.delete_many(self.kube, self.namespace, inst, first), Expect.FAULT_NOT_CLEARED
            )
        return repairs


def _reader(value: bytes | None, field: str) -> str | None:
    """The user a cached JSON value belongs to (dotted ``field``), as a string; None if absent or not JSON."""
    if value is None:
        return None
    try:
        node = json.loads(value)
        for part in field.split("."):
            node = node[part]
    except (ValueError, KeyError, TypeError):
        return None
    return None if isinstance(node, (dict, list)) else str(node)
