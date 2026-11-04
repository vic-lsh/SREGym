"""Redis incident faults: runtime state of a Redis server breaks the app while every manifest is correct.

Six kinds on one bound Redis row (``RedisInstance``; its ``faults``/key fields say what was verified
live). Each has a scripted reference and the standard repairs of the base class: restarting the
server clears every one of them (ACL rules, maxmemory and the pause are runtime-only), which is
acceptable for a cache that refills from its database and loses data on ``home-timeline-redis``, the
only copy of home timelines. Restoring the source and restarting a consumer change nothing.

* ``RedisAclRevoked``: ``ACL SETUSER default -@write|-@read``; reference ``+@all``.
* ``RedisOutOfMemory``: ``maxmemory`` far below use with ``noeviction``; reference ``maxmemory`` back.
* ``RedisWritesPaused``: ``CLIENT PAUSE <ms> WRITE``; reference ``CLIENT UNPAUSE`` (never ALL).
* ``RedisPoisonedTimelineEntry``: a non-numeric or dangling member at the top of a sorted set;
  reference ``ZREM`` of that member. ``DEL`` of the key repairs a cache but destroys a timeline.
* ``RedisTimelineKeyRenamed``: a timeline key renamed away (a silent empty timeline); reference merges
  the backup back. Deleting the "stray" backup key destroys the timeline.
* ``SocialGraphStaleCache``: a follower missing from a cached follower set (and a ghost in it); the
  service trusts a non-empty key, so posts silently skip that follower; reference ``DEL`` of the key.
"""

from __future__ import annotations

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Expect, Repair
from sregym.conductor.problems.composition.app_bindings import WriteCheck
from sregym.generators.fault.components import http, redis
from sregym.generators.fault.components.kube import ComponentError

POISON_SCORE = redis.DEFAULT_POISON_SCORE
PAUSE_MS = 3_600_000


class _RedisFault(ComponentFaultProblem):
    kind_key = ""  # the catalog key; the row's ``faults`` must list it

    def __init__(self, app_name: str, target: str, description: str, namespace=None, kube=None):
        super().__init__(app_name=app_name, target=target, description=description, namespace=namespace, kube=kube)
        if not hasattr(self.target, "faults"):
            raise ValueError(f"{target} is not a redis row of the {app_name} binding")
        if self.kind_key not in self.target.faults:
            raise ValueError(f"{self.kind_key} is not verified on {target} in the {app_name} binding")
        self.instance = self.target

    def _call(self, fn, *args, **kw):
        return fn(self.kube, self.namespace, self.instance, *args, **kw)

    def _require_rejected(self, expected: str) -> None:
        """Raise if a write is still accepted after the injection (the fault did not take)."""
        got = self._call(redis.probe_write)
        if got != expected:
            raise ComponentError(
                f"{self.instance.workload} in {self.namespace}: after injection a write probe gave {got!r}, "
                f"expected {expected!r}"
            )


class RedisAclRevoked(_RedisFault):
    kind_key = "redis_acl_revoked"
    STATE_ATTRS = ("mode",)

    def __init__(self, app_name: str, target: str, mode: str, namespace=None, kube=None):
        if mode not in redis.ACL_MODES:
            raise ValueError(f"unknown ACL mode {mode!r}; expected one of {redis.ACL_MODES}")
        self.mode = mode
        self.blocks_writes = mode == "write"
        super().__init__(
            app_name,
            target,
            f"The default user of {target} lost its @{mode} command category at runtime (ACL SETUSER default "
            f"-@{mode}), so every {mode} command through any connection fails with NOPERM while the server "
            "stays up and the other category keeps working.",
            namespace,
            kube,
        )

    def inject(self) -> None:
        self._call(redis.acl_revoke, self.mode)
        if self.mode == "write":
            self._require_rejected("NOPERM")

    def fault_cleared(self) -> tuple[bool, str]:
        modes = self._call(redis.acl_revoked_modes)
        return self.mode not in modes, f"{self.instance.workload} revoked categories: {list(modes) or 'none'}"

    def reference_mitigation(self) -> None:
        self._call(redis.acl_restore)


class RedisOutOfMemory(_RedisFault):
    kind_key = "redis_out_of_memory"
    blocks_writes = True
    STATE_ATTRS = ("previous_maxmemory",)

    def __init__(self, app_name: str, target: str, fraction: float, namespace=None, kube=None):
        redis.oom_limit(1, fraction)  # validates the fraction
        self.fraction = fraction
        self.previous_maxmemory: int | None = None
        super().__init__(
            app_name,
            target,
            f"{target} had maxmemory set to {fraction:.0%} of its current use with the noeviction policy, so "
            "every command that grows the data set fails with OOM while reads keep working.",
            namespace,
            kube,
        )

    def inject(self) -> None:
        self.previous_maxmemory = self._call(redis.get_maxmemory)
        limit = redis.oom_limit(self._call(redis.used_memory), self.fraction)
        self._call(redis.set_maxmemory, limit)
        self._require_rejected("OOM")

    def fault_cleared(self) -> tuple[bool, str]:
        if self.previous_maxmemory is None:
            return False, "nothing injected (fault not injected here)"
        now = self._call(redis.get_maxmemory)
        return (
            now == self.previous_maxmemory,
            f"{self.instance.workload} maxmemory={now} (was {self.previous_maxmemory})",
        )

    def reference_mitigation(self) -> None:
        self._call(redis.set_maxmemory, self.previous_maxmemory or 0)


class RedisWritesPaused(_RedisFault):
    kind_key = "redis_writes_paused"
    blocks_writes = True

    def __init__(self, app_name: str, target: str, duration_ms: int = PAUSE_MS, namespace=None, kube=None):
        if duration_ms < 600_000:
            raise ValueError("a pause must outlast an episode: duration_ms >= 600000")
        self.duration_ms = duration_ms
        super().__init__(
            app_name,
            target,
            f"{target} was told to pause write commands for {duration_ms // 60_000} minutes (CLIENT PAUSE WRITE): "
            "writes block until a client times out while reads are served.",
            namespace,
            kube,
        )

    def inject(self) -> None:
        self._call(redis.client_pause_write, self.duration_ms)
        self._require_rejected(redis.PAUSED)

    def fault_cleared(self) -> tuple[bool, str]:
        paused = self._call(redis.writes_paused)
        return not paused, f"{self.instance.workload} writes {'paused' if paused else 'flow'}"

    def reference_mitigation(self) -> None:
        self._call(redis.client_unpause)


class RedisPoisonedTimelineEntry(_RedisFault):
    kind_key = "redis_poisoned_timeline_entry"
    STATE_ATTRS = ("member",)

    def __init__(self, app_name: str, target: str, key: str, poison: str, token: str, namespace=None, kube=None):
        self.key, self.poison, self.token = key, poison, token
        self.member = redis.poison_member(poison, token)
        super().__init__(
            app_name,
            target,
            f"A {'non-numeric' if poison == 'non_numeric' else 'dangling'} member {self.member!r} was inserted at "
            f"the top of the sorted set {key!r} in {target}; every read of that key throws while other keys "
            "work. Removing that one member repairs it.",
            namespace,
            kube,
        )
        if key not in self.instance.poison_keys:
            raise ValueError(f"{key!r} is not a poison key of {target}")

    def inject(self) -> None:
        self._call(redis.zadd_member, self.key, self.member, POISON_SCORE)

    def fault_cleared(self) -> tuple[bool, str]:
        present = self._call(redis.has_member, self.key, self.member)
        return not present, f"member {self.member!r} {'still in' if present else 'gone from'} {self.key!r}"

    def reference_mitigation(self) -> None:
        self._call(redis.zrem_member, self.key, self.member)

    def extra_repairs(self) -> dict[str, Repair]:
        loses = self.instance.restart_loses_data
        return {
            "delete_key": Repair(
                lambda: self._call(redis.repair_stale, self.key),
                Expect.DATA_LOST if loses else Expect.PASSES,
                destroys_data=loses,
            )
        }


class RedisTimelineKeyRenamed(_RedisFault):
    kind_key = "redis_timeline_key_renamed"
    STATE_ATTRS = ("backup_key",)

    def __init__(self, app_name: str, target: str, key: str, token: str, namespace=None, kube=None):
        self.key = key
        self.backup_key = redis.renamed_key(key, token)
        super().__init__(
            app_name,
            target,
            f"The sorted set {key!r} in {target} was renamed to {self.backup_key!r}, so the timeline of user {key} "
            "reads as empty (HTTP 200, no error) while the data still exists under the new name.",
            namespace,
            kube,
        )
        if key not in self.instance.rename_keys:
            raise ValueError(f"{key!r} is not a rename key of {target}")

    def inject(self) -> None:
        self._call(redis.rename_key, self.key, self.backup_key)

    def fault_cleared(self) -> tuple[bool, str]:
        have, stray = self._call(redis.key_exists, self.key), self._call(redis.key_exists, self.backup_key)
        return have and not stray, f"{self.key!r} exists={have}, {self.backup_key!r} exists={stray}"

    def reference_mitigation(self) -> None:
        # Idempotent: after a repair already moved or deleted the backup there is nothing to merge, and
        # recovery (which runs the reference) must not fail on it; the verdict says whether the key is back.
        if self._call(redis.key_exists, self.backup_key):
            self._call(redis.merge_back, self.key, self.backup_key)

    def extra_repairs(self) -> dict[str, Repair]:
        return {
            # Correct while nothing re-created the key (the seeded user is not written during an episode).
            "rename_back": Repair(
                lambda: redis.cli(self.kube, self.namespace, self.instance, "RENAME", self.backup_key, self.key),
                Expect.PASSES,
            ),
            "delete_backup_key": Repair(
                lambda: self._call(redis.repair_stale, self.backup_key), Expect.FAULT_NOT_CLEARED, destroys_data=True
            ),
        }


class SocialGraphStaleCache(_RedisFault):
    kind_key = "social_graph_stale_cache"
    blocks_writes = True  # the verdict needs a compose to reach the follower's home timeline
    STATE_ATTRS = ("dropped",)

    def __init__(self, app_name: str, target: str, key: str, ghost: str, namespace=None, kube=None):
        self.key, self.ghost = key, ghost
        self.dropped: str | None = None
        super().__init__(
            app_name,
            target,
            f"The cached follower set {key!r} in {target} lost a follower and gained a stale entry {ghost!r}. "
            "The social-graph service trusts a non-empty cache key and never asks its database, so new posts "
            "silently skip the missing follower.",
            namespace,
            kube,
        )
        pairs = dict(self.instance.stale_sets)
        if key not in pairs:
            raise ValueError(f"{key!r} is not a stale-set key of {target}")
        self.member = pairs[key]

    def write_check(self) -> WriteCheck | None:
        return self.instance.write_check

    def prepare_target(self) -> None:
        """Make sure the follower really follows (the traffic probe's own follow write, plus its setup)."""
        entry = self.binding.entry
        follow = tuple(r for r in entry.writes if r.path.endswith("/follow"))
        http.fetch(self.kube, self.binding, self.namespace, (*entry.setup, *follow))

    def inject(self) -> None:
        if not self._call(redis.has_member, self.key, self.member):
            raise ComponentError(f"{self.key!r} in {self.instance.workload} has no member {self.member!r} to drop")
        self._call(redis.make_stale, self.key, drop=(self.member,), add=(self.ghost,))
        self.dropped = self.member

    def fault_cleared(self) -> tuple[bool, str]:
        if self.dropped is None:
            return False, "nothing injected (fault not injected here)"
        members = self._call(redis.zset_members, self.key)
        stale = self.ghost in members or (bool(members) and self.member not in members)
        return not stale, f"{self.key!r} members={members[:6]} (missing {self.member!r}, ghost {self.ghost!r})"

    def reference_mitigation(self) -> None:
        self._call(redis.repair_stale, self.key)

    def extra_repairs(self) -> dict[str, Repair]:
        def readd() -> None:
            self._call(redis.zrem_member, self.key, self.ghost)
            self._call(redis.zadd_member, self.key, self.member, 0)

        return {"readd_follower": Repair(readd, Expect.PASSES)}
