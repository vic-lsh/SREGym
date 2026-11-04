"""Redis incident-fault primitives against a fake kubectl that interprets redis-cli commands."""

from __future__ import annotations

import json

import pytest

from sregym.conductor.problems.composition.app_bindings import RedisInstance
from sregym.generators.fault.components import redis
from sregym.generators.fault.components.kube import ComponentError

INST = RedisInstance("home-timeline-redis", holds_primary_data=True)


class FakeRedisKube:
    def __init__(self):
        self.zsets: dict[str, dict[str, float]] = {"1": {"10": 1.0, "11": 2.0}}
        self.acl: set[str] = set()
        self.maxmemory = 0
        self.used = 1000
        self.paused = False

    def json(self, argv):
        return json.loads(self.run([*argv, "-o", "json"]))

    def run(self, argv, stdin=None, *, timeout_s=None):
        if argv[:2] == ["get", "deployment"]:
            return json.dumps({"spec": {"selector": {"matchLabels": {"app": argv[2]}}}}).encode()
        if argv[:2] == ["get", "pods"]:
            return json.dumps(
                {"items": [{"metadata": {"name": "p-0", "creationTimestamp": "t"}, "status": {"phase": "Running"}}]}
            ).encode()
        cmd = argv[argv.index("--") + 1 :]
        if cmd[0] == "timeout":
            cmd = cmd[2:]
            out = self._cmd(cmd[3:], timed=True)
            if out is None:
                raise ComponentError("exit 124: kubectl exec")
            return out.encode()
        return self._cmd(cmd[3:]).encode()

    def _denied(self, kind: str) -> bool:
        return kind in self.acl

    def _cmd(self, a, timed=False):
        op = a[0].upper()
        writes = {"ZADD", "ZREM", "RENAME", "DEL", "ZUNIONSTORE"}
        if op in writes and self.paused:
            return None
        if op in writes and self._denied("write"):
            return f"NOPERM this user has no permissions to run the '{op.lower()}' command\n"
        if op in {"ZREVRANGE", "ZSCORE", "ZCARD", "EXISTS"} and self._denied("read"):
            return f"NOPERM this user has no permissions to run the '{op.lower()}' command\n"
        if op in {"ZADD", "ZUNIONSTORE"} and self.maxmemory and self.used > self.maxmemory:
            return "OOM command not allowed when used memory > 'maxmemory'.\n"
        if op == "ACL" and a[1] == "SETUSER":
            rule = a[3]
            if rule == "+@all":
                self.acl.clear()
            else:
                self.acl.add(rule[2:])
            return "OK\n"
        if op == "ACL" and a[1] == "LIST":
            return "user default on nopass ~* &* +@all " + " ".join(f"-@{m}" for m in sorted(self.acl)) + "\n"
        if op == "INFO":
            return f"# Memory\r\nused_memory:{self.used}\r\n"
        if op == "CONFIG" and a[1] == "GET":
            return f"maxmemory\n{self.maxmemory}\n"
        if op == "CONFIG":
            self.maxmemory = int(a[3])
            return "OK\n"
        if op == "CLIENT":
            self.paused = a[1].upper() == "PAUSE"
            return "OK\n"
        if op == "ZADD":
            self.zsets.setdefault(a[1], {})[a[3]] = float(a[2])
            return "(integer) 1\n"
        if op == "ZREM":
            z = self.zsets.get(a[1], {})
            n = sum(z.pop(m, None) is not None for m in a[2:])
            return f"(integer) {n}\n"
        if op == "ZSCORE":
            v = self.zsets.get(a[1], {}).get(a[2])
            return "(nil)\n" if v is None else f"{v}\n"
        if op == "ZCARD":
            return f"(integer) {len(self.zsets.get(a[1], {}))}\n"
        if op == "ZREVRANGE":
            z = self.zsets.get(a[1], {})
            return "".join(m + "\n" for m in sorted(z, key=lambda m: -z[m]))
        if op == "EXISTS":
            return f"(integer) {int(a[1] in self.zsets)}\n"
        if op == "RENAME":
            if a[1] not in self.zsets:
                return "ERR no such key\n"
            self.zsets[a[2]] = self.zsets.pop(a[1])
            return "OK\n"
        if op == "DEL":
            n = int(self.zsets.pop(a[1], None) is not None)
            return f"(integer) {n}\n"
        if op == "ZUNIONSTORE":
            dst, srcs = a[1], a[3:5]
            merged: dict[str, float] = {}
            for s in srcs:
                for m, v in self.zsets.get(s, {}).items():
                    merged[m] = max(v, merged.get(m, v))
            self.zsets[dst] = merged
            return f"(integer) {len(merged)}\n"
        raise AssertionError(f"unexpected redis command {a}")


@pytest.mark.parametrize("mode", redis.ACL_MODES)
def test_acl_revoke_blocks_the_mode_and_restore_clears_it(mode):
    k = FakeRedisKube()
    redis.acl_revoke(k, "ns", INST, mode)
    assert redis.acl_revoked_modes(k, "ns", INST) == (mode,)
    blocked = (
        (lambda: redis.zadd_member(k, "ns", INST, "1", "99", 5))
        if mode == "write"
        else (lambda: redis.zset_members(k, "ns", INST, "1"))
    )
    with pytest.raises(ComponentError, match="NOPERM"):
        blocked()
    redis.acl_restore(k, "ns", INST)
    assert redis.acl_revoked_modes(k, "ns", INST) == ()
    blocked()


def test_acl_rejects_unknown_mode():
    with pytest.raises(ValueError):
        redis.acl_revoke(FakeRedisKube(), "ns", INST, "all")


def test_maxmemory_trips_writes_and_zero_restores():
    k = FakeRedisKube()
    limit = redis.oom_limit(redis.used_memory(k, "ns", INST), 0.5)
    redis.set_maxmemory(k, "ns", INST, limit)
    with pytest.raises(ComponentError, match="OOM"):
        redis.zadd_member(k, "ns", INST, "1", "99", 5)
    assert redis.get_maxmemory(k, "ns", INST) == limit
    redis.set_maxmemory(k, "ns", INST, 0)
    redis.zadd_member(k, "ns", INST, "1", "99", 5)


def test_pause_is_detected_by_a_timed_out_write_probe_and_unpause_clears():
    k = FakeRedisKube()
    assert not redis.writes_paused(k, "ns", INST)
    redis.client_pause_write(k, "ns", INST, 3_600_000)
    assert redis.writes_paused(k, "ns", INST)
    redis.client_unpause(k, "ns", INST)
    assert not redis.writes_paused(k, "ns", INST)


def test_pause_duration_must_be_positive():
    with pytest.raises(ValueError):
        redis.client_pause_write(FakeRedisKube(), "ns", INST, 0)


def test_poison_members_and_surgical_zrem():
    k = FakeRedisKube()
    for kind, token in (("non_numeric", "x1"), ("dangling_id", "999999")):
        member = redis.poison_member(kind, token)
        redis.zadd_member(k, "ns", INST, "1", member, redis.DEFAULT_POISON_SCORE)
        assert redis.zset_members(k, "ns", INST, "1", 0, 0)[0] == member
        assert redis.has_member(k, "ns", INST, "1", member)
        assert redis.zrem_member(k, "ns", INST, "1", member) == 1
        assert not redis.has_member(k, "ns", INST, "1", member)
    assert redis.zset_members(k, "ns", INST, "1") == ["11", "10"]


def test_poison_member_validates():
    with pytest.raises(ValueError):
        redis.poison_member("dangling_id", "abc")
    with pytest.raises(ValueError):
        redis.poison_member("bogus", "1")


def test_rename_hides_the_key_and_merge_back_keeps_newer_writes():
    k = FakeRedisKube()
    bak = redis.renamed_key("1", "t")
    redis.rename_key(k, "ns", INST, "1", bak)
    assert not redis.key_exists(k, "ns", INST, "1")
    assert redis.zset_members(k, "ns", INST, "1") == []
    redis.zadd_member(k, "ns", INST, "1", "12", 3)  # a compose after the rename re-creates the key
    redis.merge_back(k, "ns", INST, "1", bak)
    assert redis.zset_members(k, "ns", INST, "1") == ["12", "11", "10"]
    assert not redis.key_exists(k, "ns", INST, bak)


def test_rename_refuses_to_overwrite_and_missing_backup_raises():
    k = FakeRedisKube()
    k.zsets["b"] = {"1": 1}
    with pytest.raises(ComponentError, match="overwrite"):
        redis.rename_key(k, "ns", INST, "1", "b")
    with pytest.raises(ComponentError, match="does not exist"):
        redis.merge_back(k, "ns", INST, "1", "nope")


def test_stale_followers_keep_key_non_empty_and_del_repairs():
    k = FakeRedisKube()
    k.zsets["9100:followers"] = {"9101": 1, "9102": 1}
    redis.make_stale(k, "ns", INST, "9100:followers", drop=("9101",), add=("7777",))
    assert set(redis.zset_members(k, "ns", INST, "9100:followers")) == {"9102", "7777"}
    assert redis.zcard(k, "ns", INST, "9100:followers") == 2
    redis.repair_stale(k, "ns", INST, "9100:followers")
    assert not redis.key_exists(k, "ns", INST, "9100:followers")
    with pytest.raises(ValueError):
        redis.make_stale(k, "ns", INST, "9100:followers", drop=())


@pytest.mark.parametrize("fraction", [0, -1, 0.51, 1.0])
def test_oom_limit_rejects_fractions_that_do_not_trip_deterministically(fraction):
    with pytest.raises(ValueError):
        redis.oom_limit(1000, fraction)


def test_probe_write_names_the_rejection_and_cleans_up_after_success():
    k = FakeRedisKube()
    assert redis.probe_write(k, "ns", INST) is None
    assert "__sregym_write_probe__" not in k.zsets
    redis.acl_revoke(k, "ns", INST, "write")
    assert redis.probe_write(k, "ns", INST) == "NOPERM"
    redis.acl_restore(k, "ns", INST)
    redis.set_maxmemory(k, "ns", INST, 1)
    assert redis.probe_write(k, "ns", INST) == "OOM"
    redis.set_maxmemory(k, "ns", INST, 0)
    redis.client_pause_write(k, "ns", INST, 1000)
    assert redis.probe_write(k, "ns", INST) == redis.PAUSED
