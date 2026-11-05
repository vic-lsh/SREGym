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

Poisoned entry, renamed key and stale cache take a ``scope`` (``problems/scope.py``): ``single`` is the original
one-record fault (a fixed key, only SREGym's own probe users); ``wide`` and ``partial`` pick their records from
the row's per-user keyspace (``RedisInstance.keyspace``: the keys the app really holds for its seeded users, never
the probe or fault users), so a fault hits every user or a seeded random fraction. The verdict then also reads a
spread sample of the damaged records (timelines) or runs compose-and-read for two damaged authors (follower sets),
and ``fault_cleared`` counts every selected key: fixing one key is a decoy.
"""

from __future__ import annotations

import random
import re

from sregym.conductor.problems import scope as scopes
from sregym.conductor.problems.component_fault import ComponentFaultProblem, Expect, Repair
from sregym.conductor.problems.composition.app_bindings import WriteCheck
from sregym.conductor.problems.composition.grading import Request
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


COMPOSE_PATH = "/wrk2-api/post/compose"
SAMPLE_READS = 3  # damaged timelines the verdict reads back
SAMPLE_CHECKS = 2  # damaged authors the verdict composes as
WARM_AUTHORS = 40  # composes that refill a cold follower cache


class _ScopedKeys:
    """Mixin of the three scoped kinds: choose the damaged keys from the live keyspace while healthy
    (``prepare_target``), persist them (``STATE_ATTRS`` includes ``scoped_keys``), and build the verdict's reads."""

    scope: str
    fraction: float | None
    pick_seed: int
    scoped_keys: tuple[str, ...]
    members: dict[str, tuple[str, ...]]  # followers of each selected author, before the fault

    def _init_scope(self, scope: str, fraction: float | None, pick_seed: int, key: str | None) -> None:
        scopes.validate(scope, fraction)
        self.scope, self.fraction, self.pick_seed = scope, fraction, pick_seed
        self.scoped_keys, self.members = (), {}
        if scope == "single" and key is None:
            raise ValueError("single scope needs the key")
        if scope != "single" and key is not None:
            raise ValueError(f"a {scope} scope picks its keys from the keyspace; do not pass key")

    @property
    def scoped(self) -> bool:
        return self.scope != "single"

    def _check_scope_supported(self) -> None:
        if self.scoped and (self.instance.keyspace is None or self.kind_key not in self.instance.scoped_faults):
            raise ValueError(f"{self.kind_key} has no {self.scope} scope on {self.instance.workload}")

    def _eligible(self) -> list[str]:
        space = self.instance.keyspace
        assert space is not None
        keys = self._call(redis.scan_keys, space.glob)
        if len(keys) < space.min_keys and space.warm_from:
            self._warm(space)
            keys = self._call(redis.scan_keys, space.glob)
        eligible = [k for k in keys if (m := re.fullmatch(space.regex, k)) and int(m["uid"]) < space.uid_below]
        if len(eligible) < space.min_keys:
            raise ComponentError(
                f"{self.instance.workload} in {self.namespace}: {len(eligible)} seeded keys match {space.glob!r}, "
                f"need {space.min_keys} for a {self.scope} scope"
            )
        if len(eligible) > redis.MAX_SCOPED_KEYS:
            eligible = sorted(random.Random(self.pick_seed).sample(eligible, redis.MAX_SCOPED_KEYS))
        return eligible

    def _warm(self, space) -> None:
        """A restarted cache is cold until its keys are read: compose as users the seeded data holds (real writes
        through the entry point that make the service refill each author's follower set from its database)."""
        source = self.binding.row(space.warm_from, type(self.instance))
        users = [
            m["uid"]
            for k in redis.scan_keys(self.kube, self.namespace, source, "[0-9]*")
            if (m := re.fullmatch(r"(?P<uid>\d+)", k)) and int(m["uid"]) < space.uid_below
        ][:WARM_AUTHORS]
        if not users:
            raise ComponentError(f"{space.warm_from} holds no seeded user keys to warm {self.instance.workload} from")
        http.fetch(self.kube, self.binding, self.namespace, tuple(self._compose(u, "sregym warm") for u in users))

    def _compose(self, uid: str, text: str) -> Request:
        space = self.instance.keyspace
        assert space is not None
        return Request(
            COMPOSE_PATH,
            (("user_id", uid), ("username", space.username.format(uid)), ("post_type", "0"), ("text", text)),
        )

    def _select(self, pool: list[str]) -> tuple[str, ...]:
        return scopes.select(pool, self.scope, self.fraction, self.pick_seed)

    def _read_members(self, keys: list[str]) -> None:
        self.members = self._call(redis.members_of, tuple(keys))

    # verdict reads --------------------------------------------------------------------------------

    def _uid(self, key: str) -> str:
        space = self.instance.keyspace
        assert space is not None
        m = re.fullmatch(space.regex, key)
        assert m is not None
        return m["uid"]

    def _timeline_reads(self) -> tuple[str, ...]:
        space = self.instance.keyspace
        if not self.scoped or space is None or space.read is None:
            return ()
        return tuple(space.read.format(uid=self._uid(k)) for k in scopes.spread(self.scoped_keys, SAMPLE_READS))

    def _follower_checks(self, followers: dict[str, str]) -> tuple[WriteCheck, ...]:
        """Compose as a damaged author, read a follower's home timeline: ``followers`` maps author key -> follower."""
        space = self.instance.keyspace
        if not self.scoped or space is None or space.follower_read is None:
            return ()
        checks = []
        for key in scopes.spread(tuple(k for k in self.scoped_keys if k in followers), SAMPLE_CHECKS):
            uid = self._uid(key)
            checks.append(
                WriteCheck(
                    COMPOSE_PATH,
                    (("user_id", uid), ("username", space.username.format(uid)), ("post_type", "0"), ("text", "")),
                    "text",
                    space.follower_read.format(uid=followers[key]),
                )
            )
        return tuple(checks)


class RedisAclRevoked(_RedisFault):
    kind_key = "redis_acl_revoked"
    restricts_identity = True
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


class RedisPoisonedTimelineEntry(_ScopedKeys, _RedisFault):
    kind_key = "redis_poisoned_timeline_entry"
    STATE_ATTRS = ("member", "scoped_keys")

    def __init__(
        self,
        app_name: str,
        target: str,
        poison: str,
        token: str,
        key: str | None = None,
        scope: str = "single",
        fraction: float | None = None,
        pick_seed: int = 0,
        namespace=None,
        kube=None,
    ):
        self._init_scope(scope, fraction, pick_seed, key)
        self.key, self.poison, self.token = key, poison, token
        self.member = redis.poison_member(poison, token)
        what = "non-numeric" if poison == "non_numeric" else "dangling"
        where = (
            f"the sorted set {key!r}"
            if key is not None
            else f"{'every' if scope == 'wide' else 'a fraction of the'} per-user sorted set"
        )
        super().__init__(
            app_name,
            target,
            f"A {what} member {self.member!r} was inserted at the top of {where} in {target}; every read of an "
            "affected key throws while other keys work. Removing that one member from each repairs it.",
            namespace,
            kube,
        )
        self._check_scope_supported()
        if key is not None and key not in self.instance.poison_keys:
            raise ValueError(f"{key!r} is not a poison key of {target}")
        # a damaged follower set breaks the compose that fans out to it: the writes check shows it
        self.blocks_writes = self.scoped and self.instance.keyspace.follower_read is not None
        self.followers: dict[str, str] = {}

    def prepare_target(self) -> None:
        if not self.scoped:
            return
        self.scoped_keys = self._select(self._eligible())
        if self.instance.keyspace.follower_read is not None:
            self._read_members(list(self.scoped_keys))
            self.scoped_keys = tuple(k for k in self.scoped_keys if self.members.get(k))
            if not self.scoped_keys:
                raise ComponentError(f"{self.instance.workload}: no selected author has a follower")
            self.followers = {k: self.members[k][0] for k in self.scoped_keys}

    def extra_reads(self) -> tuple[str, ...]:
        return self._timeline_reads()

    def write_checks(self) -> tuple[WriteCheck, ...]:
        return self._follower_checks(self.followers) if self.scoped else super().write_checks()

    def inject(self) -> None:
        if not self.scoped:
            self._call(redis.zadd_member, self.key, self.member, POISON_SCORE)
            return
        if not self.scoped_keys:
            raise ComponentError("scoped keys were not chosen (prepare_injection did not run)")
        self._call(redis.eval_batch, redis.POISON_ALL, self.scoped_keys, self.member, f"{POISON_SCORE:.0f}")

    def fault_cleared(self) -> tuple[bool, str]:
        if not self.scoped:
            present = self._call(redis.has_member, self.key, self.member)
            return not present, f"member {self.member!r} {'still in' if present else 'gone from'} {self.key!r}"
        if not self.scoped_keys:
            return False, "nothing injected (fault not injected here)"
        left = self._call(redis.eval_batch, redis.COUNT_MEMBER, self.scoped_keys, self.member, read_only=True)
        return left == 0, f"member {self.member!r} still in {left}/{len(self.scoped_keys)} {self.scope} keys"

    def reference_mitigation(self) -> None:
        if not self.scoped:
            self._call(redis.zrem_member, self.key, self.member)
        elif self.scoped_keys:
            self._call(redis.eval_batch, redis.REMOVE_MEMBER_ALL, self.scoped_keys, self.member)

    def extra_repairs(self) -> dict[str, Repair]:
        loses = self.instance.restart_loses_data
        if not self.scoped:
            return {
                "delete_key": Repair(
                    lambda: self._call(redis.repair_stale, self.key),
                    Expect.DATA_LOST if loses else Expect.PASSES,
                    destroys_data=loses,
                )
            }
        return {
            # one record fixed, the rest of the incident left: the obvious first key of a wide incident
            "remove_member_first_key": Repair(
                lambda: self._call(redis.zrem_member, self.scoped_keys[0], self.member), Expect.FAULT_NOT_CLEARED
            ),
            "delete_keys": Repair(
                lambda: self._call(redis.eval_batch, redis.DELETE_ALL, self.scoped_keys),
                Expect.DATA_LOST if loses else Expect.PASSES,
                destroys_data=loses,
            ),
        }


class RedisTimelineKeyRenamed(_ScopedKeys, _RedisFault):
    kind_key = "redis_timeline_key_renamed"
    STATE_ATTRS = ("backup_key", "scoped_keys")

    def __init__(
        self,
        app_name: str,
        target: str,
        token: str,
        key: str | None = None,
        scope: str = "single",
        fraction: float | None = None,
        pick_seed: int = 0,
        namespace=None,
        kube=None,
    ):
        self._init_scope(scope, fraction, pick_seed, key)
        self.key, self.token = key, token
        self.backup_key = redis.renamed_key(key, token) if key is not None else None
        self.suffix = redis.backup_suffix(token)
        what = (
            f"The sorted set {key!r} in {target} was renamed to {self.backup_key!r}, so the timeline of user {key}"
            if key is not None
            else f"{'Every' if scope == 'wide' else 'A fraction of the'} per-user timeline key in {target} was "
            f"renamed with the suffix {self.suffix!r} (a botched key migration), so those users' timelines"
        )
        super().__init__(
            app_name,
            target,
            f"{what} read as empty (HTTP 200, no error) while the data still exists under the new names.",
            namespace,
            kube,
        )
        self._check_scope_supported()
        if key is not None and key not in self.instance.rename_keys:
            raise ValueError(f"{key!r} is not a rename key of {target}")

    def prepare_target(self) -> None:
        if self.scoped:
            self.scoped_keys = self._select(self._eligible())

    def extra_reads(self) -> tuple[str, ...]:
        return self._timeline_reads()

    def inject(self) -> None:
        if not self.scoped:
            self._call(redis.rename_key, self.key, self.backup_key)
            return
        if not self.scoped_keys:
            raise ComponentError("scoped keys were not chosen (prepare_injection did not run)")
        self._call(redis.eval_batch, redis.RENAME_ALL, self.scoped_keys, self.suffix)

    def fault_cleared(self) -> tuple[bool, str]:
        # The fault's own state is the renamed copy. A repair that removed it without bringing the key back
        # (a restart, deleting the "stray" key) cleared the state but lost the data: the data check says so.
        if not self.scoped:
            have, stray = self._call(redis.key_exists, self.key), self._call(redis.key_exists, self.backup_key)
            return not stray, f"{self.key!r} exists={have}, {self.backup_key!r} exists={stray}"
        if not self.scoped_keys:
            return False, "nothing injected (fault not injected here)"
        stray = self._call(
            redis.eval_batch, redis.COUNT_SUFFIXED_EXISTING, self.scoped_keys, self.suffix, read_only=True
        )
        return stray == 0, f"{stray}/{len(self.scoped_keys)} renamed keys still under {self.suffix!r}"

    def reference_mitigation(self) -> None:
        # Idempotent: after a repair already moved or deleted the backup there is nothing to merge, and
        # recovery (which runs the reference) must not fail on it; the verdict says whether the key is back.
        if self.scoped:
            if self.scoped_keys:
                self._call(redis.eval_batch, redis.MERGE_BACK_ALL, self.scoped_keys, self.suffix)
        elif self._call(redis.key_exists, self.backup_key):
            self._call(redis.merge_back, self.key, self.backup_key)

    def extra_repairs(self) -> dict[str, Repair]:
        if self.scoped:
            return {
                "rename_back": Repair(
                    lambda: self._call(redis.eval_batch, redis.RENAME_BACK_ALL, self.scoped_keys, self.suffix),
                    Expect.PASSES,
                ),
                "merge_first_key": Repair(
                    lambda: self._call(redis.eval_batch, redis.MERGE_BACK_ALL, self.scoped_keys[:1], self.suffix),
                    Expect.FAULT_NOT_CLEARED,
                ),
                "delete_backup_keys": Repair(
                    lambda: self._call(redis.eval_batch, redis.DELETE_SUFFIXED_ALL, self.scoped_keys, self.suffix),
                    Expect.DATA_LOST,
                    destroys_data=True,
                ),
            }
        return {
            # Correct while nothing re-created the key (the seeded user is not written during an episode).
            "rename_back": Repair(
                lambda: redis.cli(self.kube, self.namespace, self.instance, "RENAME", self.backup_key, self.key),
                Expect.PASSES,
            ),
            "delete_backup_key": Repair(
                lambda: self._call(redis.repair_stale, self.backup_key), Expect.DATA_LOST, destroys_data=True
            ),
        }


class SocialGraphStaleCache(_ScopedKeys, _RedisFault):
    kind_key = "social_graph_stale_cache"
    blocks_writes = True  # the verdict needs a compose to reach the follower's home timeline
    STATE_ATTRS = ("dropped", "scoped_keys", "dropped_by_key")

    def __init__(
        self,
        app_name: str,
        target: str,
        ghost: str,
        key: str | None = None,
        scope: str = "single",
        fraction: float | None = None,
        pick_seed: int = 0,
        namespace=None,
        kube=None,
    ):
        self._init_scope(scope, fraction, pick_seed, key)
        self.key, self.ghost = key, ghost
        self.dropped: str | None = None
        self.dropped_by_key: dict[str, str] = {}
        where = (
            f"The cached follower set {key!r} in {target} lost a follower"
            if key is not None
            else f"{'Every' if scope == 'wide' else 'A fraction of the'} cached follower set in {target} lost one "
            "follower (cache invalidation on follow/unfollow stopped working)"
        )
        super().__init__(
            app_name,
            target,
            f"{where} and gained a stale entry {ghost!r}. The social-graph service trusts a non-empty cache key and "
            "never asks its database, so new posts silently skip the missing follower.",
            namespace,
            kube,
        )
        self._check_scope_supported()
        if key is not None:
            pairs = dict(self.instance.stale_sets)
            if key not in pairs:
                raise ValueError(f"{key!r} is not a stale-set key of {target}")
            self.member = pairs[key]

    def write_check(self) -> WriteCheck | None:
        return self.instance.stale_write_check or self.instance.write_check

    def write_checks(self) -> tuple[WriteCheck, ...]:
        return self._follower_checks(self.dropped_by_key) if self.scoped else super().write_checks()

    def prepare_target(self) -> None:
        """Single: make sure the dropped follower exists and follows the author. It is the fault's own user: the
        traffic probe re-adds only its own follower, so the probe cannot heal this symptom. Scoped: choose the
        authors from the seeded follower sets and the follower each loses."""
        if not self.scoped:
            entry = self.binding.entry
            http.fetch(self.kube, self.binding, self.namespace, (*entry.setup, *self.instance.stale_setup))
            return
        pool = self._eligible()
        self._read_members(pool)
        keep = [k for k in pool if len(self.members.get(k, ())) >= 2]  # the key must stay non-empty
        self.scoped_keys = self._select(keep)
        self.dropped_by_key = {
            k: random.Random(f"{self.pick_seed}:{k}").choice(sorted(self.members[k])) for k in self.scoped_keys
        }

    def _drops(self, keys: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(self.dropped_by_key[k] for k in keys)

    def inject(self) -> None:
        if self.scoped:
            if not self.scoped_keys:
                raise ComponentError("scoped keys were not chosen (prepare_injection did not run)")
            self._call(redis.eval_batch, redis.STALE_ALL, self.scoped_keys, *self._drops(self.scoped_keys), self.ghost)
            self.dropped = self.dropped_by_key[self.scoped_keys[0]]
            return
        if not self._call(redis.has_member, self.key, self.member):
            raise ComponentError(f"{self.key!r} in {self.instance.workload} has no member {self.member!r} to drop")
        self._call(redis.make_stale, self.key, drop=(self.member,), add=(self.ghost,))
        self.dropped = self.member

    def fault_cleared(self) -> tuple[bool, str]:
        if self.dropped is None:
            return False, "nothing injected (fault not injected here)"
        if self.scoped:
            keys = self.scoped_keys
            stale = self._call(
                redis.eval_batch, redis.COUNT_STALE, keys, *self._drops(keys), self.ghost, read_only=True
            )
            return stale == 0, f"{stale}/{len(keys)} cached follower sets still stale ({self.scope})"
        members = self._call(redis.zset_members, self.key)
        stale = self.ghost in members or (bool(members) and self.member not in members)
        return not stale, f"{self.key!r} members={members[:6]} (missing {self.member!r}, ghost {self.ghost!r})"

    def reference_mitigation(self) -> None:
        if not self.scoped:
            self._call(redis.repair_stale, self.key)
        elif self.scoped_keys:
            self._call(redis.eval_batch, redis.DELETE_ALL, self.scoped_keys)

    def extra_repairs(self) -> dict[str, Repair]:
        if self.scoped:
            # the repairs are listed before the keys are chosen (prepare_injection): resolve them when run
            def readd(keys: tuple[str, ...]) -> None:
                self._call(redis.eval_batch, redis.READD_ALL, keys, *self._drops(keys), self.ghost)

            return {
                "readd_follower": Repair(lambda: readd(self.scoped_keys), Expect.PASSES),
                # one author's set repaired, the rest of the incident left
                "readd_first_author": Repair(lambda: readd(self.scoped_keys[:1]), Expect.FAULT_NOT_CLEARED),
            }

        def readd() -> None:
            self._call(redis.zrem_member, self.key, self.ghost)
            self._call(redis.zadd_member, self.key, self.member, 0)

        return {"readd_follower": Repair(readd, Expect.PASSES)}
