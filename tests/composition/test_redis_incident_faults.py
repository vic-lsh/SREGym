"""Redis incident-fault problem classes: contract against a fake kubectl that speaks redis-cli."""

from __future__ import annotations

import random
from types import SimpleNamespace

import pytest
from test_component_faults import FakeKube, offline  # noqa: F401  (offline is a fixture)
from test_redis_primitives import FakeRedisKube

from sregym.conductor.problems.component_fault import Expect
from sregym.conductor.problems.composition import app_bindings
from sregym.conductor.problems.composition.catalog import component_mongo, component_redis, component_state

pytestmark = pytest.mark.usefixtures("offline")

KEYS = tuple(e.key for e in component_redis.ENTRIES)
ENTRIES = {e.key: e for e in component_redis.ENTRIES}


def _build(key, params, kube):
    problem = ENTRIES[key].build(SimpleNamespace(app="social_network", param_dict=lambda: params))
    problem.kube = kube
    return problem


def _kube():
    k = FakeRedisKube()
    k.zsets = {"9101": {"1": 1.0, "2": 2.0}, "9100": {"1": 1.0}, "9200:followers": {"9210": 5.0, "9300": 6.0}}
    return k


@pytest.mark.parametrize("key", KEYS)
def test_every_draw_builds_a_problem_on_a_verified_target(key):
    rows = {r.workload: r for r in app_bindings.SOCIAL_NETWORK.redis}
    seen = set()
    for seed in range(40):
        draw = ENTRIES[key].sample("social_network", random.Random(seed))
        (fp,) = draw.footprints
        assert fp.field is None and fp.kind == "Deployment"  # the state lives in the pod
        params = draw.param_dict()
        assert key in rows[params["target"]].faults
        ENTRIES[key].build(draw)
        seen.add(params["target"])
    assert seen == {w for w, r in rows.items() if key in r.faults}


def test_incident_keys_are_generation_two():
    for key in KEYS:
        assert ENTRIES[key].generation("social_network") == 2


@pytest.mark.parametrize("mode", ["write", "read"])
def test_acl_reference_and_declared_repairs(mode):
    for target, restart in (("home-timeline-redis", Expect.DATA_LOST), ("user-timeline-redis", Expect.PASSES)):
        k = _kube()
        p = _build("redis_acl_revoked", {"target": target, "mode": mode}, k)
        assert p.blocks_writes == (mode == "write")
        p.inject()
        assert p.fault_cleared()[0] is False
        repairs = p.repairs()
        assert repairs["rollout_restart"].expect is restart
        assert repairs["restore_source"].expect is Expect.FAULT_NOT_CLEARED and repairs["restore_source"].no_op
        p.reference_mitigation()
        assert p.fault_cleared()[0] is True


def test_acl_write_injection_raises_when_the_revoke_did_not_take():
    k = _kube()
    p = _build("redis_acl_revoked", {"target": "home-timeline-redis", "mode": "write"}, k)
    k._denied = lambda kind: False  # the server ignores the rule
    with pytest.raises(Exception, match="write probe"):
        p.inject()


def test_oom_sets_a_limit_below_use_and_the_reference_restores_it():
    k = _kube()
    p = _build("redis_out_of_memory", {"target": "user-timeline-redis", "fraction": 0.5}, k)
    p.inject()
    assert 0 < k.maxmemory <= k.used // 2
    assert p.fault_cleared()[0] is False
    p.reference_mitigation()
    assert k.maxmemory == 0 and p.fault_cleared()[0] is True
    with pytest.raises(ValueError):
        _build("redis_out_of_memory", {"target": "user-timeline-redis", "fraction": 0.9}, k)


def test_pause_blocks_writes_until_unpaused_and_rejects_short_pauses():
    k = _kube()
    p = _build("redis_writes_paused", {"target": "home-timeline-redis", "duration_ms": 3_600_000}, k)
    p.inject()
    assert k.paused and p.fault_cleared()[0] is False
    p.reference_mitigation()
    assert not k.paused and p.fault_cleared()[0] is True
    with pytest.raises(ValueError):
        _build("redis_writes_paused", {"target": "home-timeline-redis", "duration_ms": 1000}, k)


@pytest.mark.parametrize("poison", ["non_numeric", "dangling_id"])
def test_poison_reference_is_surgical_and_delete_key_is_data_loss_only_on_the_primary(poison):
    token = "abcd1234" if poison == "non_numeric" else "9000000001"
    k = _kube()
    p = _build(
        "redis_poisoned_timeline_entry",
        {"target": "home-timeline-redis", "key": "9101", "poison": poison, "token": token},
        k,
    )
    p.inject()
    assert p.fault_cleared()[0] is False
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True and set(k.zsets["9101"]) == {"1", "2"}  # the timeline is untouched
    assert p.extra_repairs()["delete_key"].expect is Expect.DATA_LOST and p.extra_repairs()["delete_key"].destroys_data
    cache = _build(
        "redis_poisoned_timeline_entry",
        {"target": "user-timeline-redis", "key": "9100", "poison": poison, "token": token},
        k,
    )
    assert cache.extra_repairs()["delete_key"].expect is Expect.PASSES
    with pytest.raises(ValueError, match="not a poison key"):
        _build(
            "redis_poisoned_timeline_entry",
            {"target": "home-timeline-redis", "key": "1", "poison": poison, "token": token},
            k,
        )


def test_rename_is_cleared_only_when_the_key_is_back_and_the_backup_gone():
    k = _kube()
    p = _build("redis_timeline_key_renamed", {"target": "home-timeline-redis", "key": "9101", "token": "a1b2c3"}, k)
    p.inject()
    assert "9101" not in k.zsets and p.fault_cleared()[0] is False
    repairs = p.extra_repairs()
    assert repairs["delete_backup_key"].expect is Expect.DATA_LOST and repairs["delete_backup_key"].destroys_data
    assert repairs["rename_back"].expect is Expect.PASSES
    repairs["delete_backup_key"].run()  # state gone, data gone: cleared, and the data check reports the loss
    assert p.fault_cleared()[0] is True and "9101" not in k.zsets
    k.zsets["9101:bak-a1b2c3"] = {"1": 1.0, "2": 2.0}
    k.zsets["9101"] = {"99": 9.0}  # a write re-created the key meanwhile
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True and set(k.zsets["9101"]) == {"1", "2", "99"}  # nothing lost
    p.reference_mitigation()  # recovery after a repair already ran: a no-op, not an error
    assert set(k.zsets["9101"]) == {"1", "2", "99"}


def test_stale_cache_drops_the_follower_adds_a_ghost_and_del_clears_it():
    k = _kube()
    p = _build(
        "social_graph_stale_cache", {"target": "social-graph-redis", "key": "9200:followers", "ghost": "9203"}, k
    )
    assert p.fault_cleared()[0] is False  # not injected here
    p.inject()
    assert set(k.zsets["9200:followers"]) == {"9300", "9203"}
    assert p.fault_cleared()[0] is False
    assert p.write_check() is app_bindings.SOCIAL_NETWORK.row("social-graph-redis").stale_write_check
    p.reference_mitigation()
    assert "9200:followers" not in k.zsets and p.fault_cleared()[0] is True
    k.zsets["9200:followers"] = {"9300": 6.0, "9203": 0.0}
    p.repairs()["readd_follower"].run()
    assert p.fault_cleared()[0] is True and "9210" in k.zsets["9200:followers"]


def test_kinds_refuse_rows_that_do_not_list_them():
    k = _kube()
    with pytest.raises(ValueError, match="not verified"):
        _build("redis_timeline_key_renamed", {"target": "user-timeline-redis", "key": "9100", "token": "x"}, k)


# --- the access-control class rule -------------------------------------------------------------
# A fault that restricts an identity must not be judged through that identity: its verdict, witness
# and integrity reads use the driver's privileged check channel (Redis: the sregym_check user;
# Mongo: the binding's admin credentials, which no Mongo fault restricts).


DATA_READS = {"EXISTS", "SCAN", "DBSIZE", "ZSCORE", "ZREVRANGE", "ZCARD", "GET"}


class AclBlindKube(FakeKube):
    """The ``default`` user has lost both its read and its write categories (the worst case of any
    access-control kind). Anything run as another identity (``env REDISCLI_AUTH=.. --user``) is served."""

    def __init__(self):
        super().__init__()
        self.default_reads: list[str] = []

    def run(self, argv, stdin=None, *, timeout_s=None):
        if argv[0] == "exec" and "--" in argv:
            cmd = argv[argv.index("--") + 1 :]
            if cmd[0] == "timeout":
                cmd = cmd[2:]
            if cmd[0] != "env" and len(cmd) > 3 and cmd[0].endswith("-cli"):
                op = cmd[3].upper()
                if op == "ACL" and cmd[4].upper() == "LIST":
                    return b"user default on nopass ~* &* +@all -@write -@read\n"
                if op in DATA_READS | {"ZADD"}:
                    if op in DATA_READS:
                        self.default_reads.append(op)
                    return f"NOPERM this user has no permissions to run the '{op.lower()}' command\n".encode()
        return super().run(argv, stdin, timeout_s=timeout_s)


def _access_control_cases():
    """(key, params) of every draw, over several seeds, whose problem class restricts an identity."""
    cases = []
    for module in (component_redis, component_mongo, component_state):
        for entry in module.ENTRIES:
            for seed in range(30):
                draw = entry.sample("social_network", random.Random(seed))
                problem = entry.build(draw)
                if problem.restricts_identity:
                    cases.append((entry.key, draw.param_dict()))
    return cases


def test_every_access_control_kind_never_judges_itself_through_the_restricted_identity():
    cases = _access_control_cases()
    assert {key for key, _ in cases} >= {"redis_acl_revoked"}, "the registry of access-control kinds is empty"
    for key, params in cases:
        kube = AclBlindKube()
        kube.homes = {"9101": [{"post_id": 1}]}
        p = _build(key, params, kube)
        p.inject_fault()
        result = p.verdict()  # raises ComponentError (-> inconclusive) if a read went through default
        assert result["fault_cleared"] is False, (key, params)
        p.recover_fault()
        assert kube.default_reads == [], f"{key} {params}: data reads ran as the restricted default user"


def test_stale_cache_member_is_not_one_the_traffic_probe_writes():
    """The traffic probe's setup/follow writes re-add its own follower to the author's follower set. If that were
    the dropped member the probe would heal the symptom between the harness's two symptom reads (live: 'no symptom
    that persisted' on every trial). The fault owns its follower and reads that follower's timeline."""
    row = app_bindings.SOCIAL_NETWORK.row("social-graph-redis")
    entry = app_bindings.SOCIAL_NETWORK.entry
    ((_, member),) = row.stale_sets
    probe_followers = {
        dict(r.form or ()).get("user_id") for r in (*entry.setup, *entry.writes) if r.path.endswith("/follow")
    }
    assert member not in probe_followers
    assert f"user_id={member}&" in row.stale_write_check.read
    follows = [dict(r.form) for r in row.stale_setup if r.path.endswith("/follow")]
    assert follows == [{"user_id": member, "followee_id": "9200"}]
