"""Scope (wide / partial / single) of the single-record data faults: pure selection, catalog generations, and the
problem classes against a fake redis-cli that runs the batch scripts by identity (the real Lua runs live)."""

from __future__ import annotations

import fnmatch
import json
import random
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_component_faults import offline  # noqa: F401  (fixture)
from test_redis_primitives import FakeRedisKube

from sregym.conductor.problems import scope
from sregym.conductor.problems.component_fault import Expect
from sregym.conductor.problems.composition import app_bindings, catalog
from sregym.conductor.problems.composition.catalog import component_redis
from sregym.conductor.problems.composition.generator import parse_plan_id, plan_for, plan_id
from sregym.generators.fault.components import redis

ENTRIES = {e.key: e for e in component_redis.ENTRIES}
FIXTURES = Path(__file__).parent / "fixtures"
SCOPED_KEYS = ("redis_poisoned_timeline_entry", "redis_timeline_key_renamed", "social_graph_stale_cache")


# --- pure selection ---------------------------------------------------------------------------------


def test_wide_selects_everything_and_partial_a_seeded_fraction():
    keys = [str(i) for i in range(40)]
    assert scope.select(keys, "wide", None, 1) == tuple(sorted(keys))
    half = scope.select(keys, "partial", 0.5, 7)
    assert len(half) == 20 and half == scope.select(keys[::-1], "partial", 0.5, 7)  # order of input is irrelevant
    assert half != scope.select(keys, "partial", 0.5, 8)
    assert set(half) <= set(keys)


def test_scope_validation():
    for bad in (("partial", None), ("partial", 1.0), ("wide", 0.5), ("single", 0.5), ("some", None)):
        with pytest.raises(ValueError):
            scope.validate(*bad)
    with pytest.raises(ValueError):
        scope.select(["1"], "single", None, 0)
    with pytest.raises(ValueError, match="more than"):
        scope.select(["1", "2"], "partial", 0.5, 0)


def test_spread_covers_the_whole_selection():
    sel = tuple(str(i) for i in range(30))
    assert scope.spread(sel, 3) == ("0", "10", "20")
    assert scope.spread(sel[:2], 3) == ("0", "1")


# --- catalog generations ----------------------------------------------------------------------------


def test_generation_three_draws_wide_or_partial_never_single_unless_opted_in(monkeypatch):
    monkeypatch.delenv(component_redis.SINGLE_SCOPE_ENV, raising=False)
    for key in SCOPED_KEYS:
        seen = set()
        for seed in range(80):
            draw = ENTRIES[key].at_generation(3).sample("social_network", random.Random(seed))
            p = draw.param_dict()
            seen.add(p["scope"])
            assert "key" not in p and (p["scope"] == "partial") == ("fraction" in p)
            row = {r.workload: r for r in app_bindings.SOCIAL_NETWORK.redis}[p["target"]]
            assert key in row.scoped_faults and row.keyspace is not None
        assert seen == {"partial", "wide"}
    monkeypatch.setenv(component_redis.SINGLE_SCOPE_ENV, "1")
    singles = [
        ENTRIES[key].at_generation(3).sample("social_network", random.Random(seed)).param_dict()
        for key in SCOPED_KEYS
        for seed in range(80)
    ]
    assert {p["scope"] for p in singles} == {"single", "partial", "wide"}
    assert all("key" in p for p in singles if p["scope"] == "single")


def test_generation_two_keeps_drawing_the_original_single_record_faults():
    fixture = json.loads((FIXTURES / "g2_scoped_kinds_plans.json").read_text())
    assert fixture
    for pid, expected in fixture.items():
        ref = parse_plan_id(pid)
        plan = plan_for(ref, catalog.entries_for(ref.app))
        got = [{"key": d.key, "params": d.param_dict(), "tier": d.tier.name} for d in plan.draws]
        assert got == expected, pid


def test_generation_three_ids_differ_from_generation_two_for_the_revised_kinds():
    changed = 0
    for seed in range(40):
        g2 = plan_for(parse_plan_id(plan_id("social_network", 3, seed, generation=2)), catalog.entries_for("social_network"))
        g3 = plan_for(parse_plan_id(plan_id("social_network", 3, seed, generation=3)), catalog.entries_for("social_network"))
        scoped = [d for d in g3.draws if d.key in SCOPED_KEYS]
        assert all("scope" in d.param_dict() for d in scoped)
        assert not any("scope" in d.param_dict() for d in g2.draws if d.key in SCOPED_KEYS)
        changed += bool(scoped)
    assert changed


# --- problems against a fake redis-cli that runs the batch scripts ------------------------------------


class ScriptKube(FakeRedisKube):
    """FakeRedisKube plus EVAL by script identity and SCAN MATCH."""

    def _cmd(self, a, timed=False, user="default"):
        op = a[0].upper()
        if op == "SCAN":
            return "0\n" + "".join(k + "\n" for k in sorted(self.zsets) if fnmatch.fnmatchcase(k, a[a.index("MATCH") + 1]))
        if op != "EVAL":
            return super()._cmd(a, timed, user)
        script, n = a[1], int(a[2])
        keys, args = list(a[3:3 + n]), list(a[3 + n:])
        z = self.zsets
        if script == redis.POISON_ALL:
            for k in keys:
                z.setdefault(k, {})[args[0]] = float(args[1])
            return f"(integer) {len(keys)}\n"
        if script == redis.REMOVE_MEMBER_ALL:
            return f"(integer) {sum(z.get(k, {}).pop(args[0], None) is not None for k in keys)}\n"
        if script == redis.COUNT_MEMBER:
            return f"(integer) {sum(args[0] in z.get(k, {}) for k in keys)}\n"
        if script == redis.RENAME_ALL:
            if any(k + args[0] in z for k in keys):
                return "ERR backup key exists\n"
            for k in keys:
                z[k + args[0]] = z.pop(k)
            return f"(integer) {len(keys)}\n"
        if script == redis.COUNT_SUFFIXED_EXISTING:
            return f"(integer) {sum(k + args[0] in z for k in keys)}\n"
        if script == redis.MERGE_BACK_ALL:
            done = 0
            for k in keys:
                if k + args[0] in z:
                    for m, v in z.pop(k + args[0]).items():
                        z.setdefault(k, {})[m] = max(v, z.get(k, {}).get(m, v))
                    done += 1
            return f"(integer) {done}\n"
        if script == redis.RENAME_BACK_ALL:
            for k in keys:
                z[k] = z.pop(k + args[0])
            return f"(integer) {len(keys)}\n"
        if script == redis.DELETE_SUFFIXED_ALL:
            return f"(integer) {sum(z.pop(k + args[0], None) is not None for k in keys)}\n"
        if script == redis.DELETE_ALL:
            return f"(integer) {sum(z.pop(k, None) is not None for k in keys)}\n"
        if script == redis.STALE_ALL:
            for i, k in enumerate(keys):
                z[k].pop(args[i], None)
                z[k][args[-1]] = 0.0
            return f"(integer) {len(keys)}\n"
        if script == redis.COUNT_STALE:
            stale = sum(k in z and (args[-1] in z[k] or args[i] not in z[k]) for i, k in enumerate(keys))
            return f"(integer) {stale}\n"
        if script == redis.READD_ALL:
            for i, k in enumerate(keys):
                z[k].pop(args[-1], None)
                z[k][args[i]] = 0.0
            return f"(integer) {len(keys)}\n"
        if script == redis.MEMBERS_OF:
            return "".join(f"{k} {','.join(sorted(z.get(k, {})))}\n" for k in keys)
        raise AssertionError(f"unknown script {script[:60]}")


def _kube():
    k = ScriptKube()
    k.zsets = {str(u): {str(100 + u): float(u), str(200 + u): 1.0 + u} for u in range(20)}
    k.zsets.update({"9101": {"1": 1.0}, "9200": {"1": 1.0}})  # SREGym's own users: never chosen
    k.sg = {f"{u}:followers": {str(u + 1): 1.0, str(u + 2): 2.0, str(u + 3): 3.0} for u in range(20)}
    k.sg["9200:followers"] = {"9201": 1.0, "9210": 2.0}
    return k


def _build(key, params, kube):
    problem = ENTRIES[key].build(SimpleNamespace(app="social_network", param_dict=lambda: params))
    problem.kube = kube
    return problem


def _social_graph_kube():
    k = _kube()
    k.zsets = k.sg
    return k


def _wide(key, target, **extra):
    base = {"target": target, "scope": "wide", "pick_seed": 3}
    return {**base, **extra}


@pytest.mark.usefixtures("offline")
def test_wide_poison_hits_every_seeded_key_and_never_a_probe_user():
    k = _kube()
    p = _build("redis_poisoned_timeline_entry", _wide("x", "home-timeline-redis", poison="non_numeric", token="ab12"), k)
    p.prepare_target()
    assert set(p.scoped_keys) == {str(u) for u in range(20)} and "9101" not in p.scoped_keys
    p.inject()
    assert all(p.member in k.zsets[key] for key in p.scoped_keys) and p.fault_cleared()[0] is False
    assert len(p.extra_reads()) == 3 and all("home-timeline/read" in r for r in p.extra_reads())
    repairs = p.repairs()
    assert repairs["remove_member_first_key"].expect is Expect.FAULT_NOT_CLEARED
    assert repairs["delete_keys"].expect is Expect.DATA_LOST and repairs["delete_keys"].destroys_data
    repairs["remove_member_first_key"].run()
    assert p.fault_cleared()[0] is False  # one fixed record is not a repair of a wide incident
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True and all(p.member not in z for z in k.zsets.values())


@pytest.mark.usefixtures("offline")
def test_partial_poison_hits_a_fraction_and_the_same_keys_for_the_same_seed():
    k = _kube()
    params = {"target": "home-timeline-redis", "scope": "partial", "fraction": 0.5, "pick_seed": 11,
              "poison": "dangling_id", "token": "9000000001"}
    p = _build("redis_poisoned_timeline_entry", params, k)
    p.prepare_target()
    assert len(p.scoped_keys) == 10
    q = _build("redis_poisoned_timeline_entry", params, k)
    q.prepare_target()
    assert q.scoped_keys == p.scoped_keys
    p.inject()
    hit = {key for key, z in k.zsets.items() if p.member in z}
    assert hit == set(p.scoped_keys)


@pytest.mark.usefixtures("offline")
def test_poison_on_follower_sets_is_checked_by_compose_and_read_as_damaged_authors():
    k = _social_graph_kube()
    p = _build("redis_poisoned_timeline_entry", _wide("x", "social-graph-redis", poison="non_numeric", token="ab12"), k)
    assert p.blocks_writes
    p.prepare_target()
    assert "9200:followers" not in p.scoped_keys and len(p.scoped_keys) == 20
    checks = p.write_checks()
    assert len(checks) == 2
    for c in checks:
        fields = dict(c.fields)
        assert fields["user_id"].isdigit() and c.marker_field == "text" and "home-timeline/read" in c.read
        assert f"{fields['user_id']}:followers" in p.scoped_keys


@pytest.mark.usefixtures("offline")
def test_wide_rename_is_cleared_only_when_every_backup_is_merged_back():
    k = _kube()
    p = _build("redis_timeline_key_renamed", _wide("x", "home-timeline-redis", token="a1b2c3"), k)
    p.prepare_target()
    before = {key: dict(z) for key, z in k.zsets.items()}
    p.inject()
    assert not any(key in k.zsets for key in p.scoped_keys) and p.fault_cleared()[0] is False
    repairs = p.repairs()
    assert repairs["delete_backup_keys"].expect is Expect.DATA_LOST and repairs["delete_backup_keys"].destroys_data
    assert repairs["merge_first_key"].expect is Expect.FAULT_NOT_CLEARED
    repairs["merge_first_key"].run()
    assert p.fault_cleared()[0] is False
    k.zsets[p.scoped_keys[1]] = {"999": 9.0}  # a write re-created a key meanwhile: the merge keeps both
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True
    assert set(k.zsets[p.scoped_keys[1]]) == set(before[p.scoped_keys[1]]) | {"999"}
    p.reference_mitigation()  # idempotent


@pytest.mark.usefixtures("offline")
def test_wide_stale_cache_drops_one_follower_per_author_and_only_a_full_repair_clears_it():
    k = _social_graph_kube()
    p = _build("social_graph_stale_cache", _wide("x", "social-graph-redis", ghost="9202"), k)
    p.prepare_target()
    assert len(p.scoped_keys) == 20 and "9200:followers" not in p.scoped_keys
    before = {key: set(z) for key, z in k.zsets.items()}
    p.inject()
    for key in p.scoped_keys:
        assert "9202" in k.zsets[key] and p.dropped_by_key[key] not in k.zsets[key] and len(k.zsets[key]) >= 2
        assert p.dropped_by_key[key] in before[key]
    assert p.fault_cleared()[0] is False and p.blocks_writes
    repairs = p.repairs()
    assert repairs["readd_first_author"].expect is Expect.FAULT_NOT_CLEARED
    repairs["readd_first_author"].run()
    assert p.fault_cleared()[0] is False
    repairs["readd_follower"].run()
    assert p.fault_cleared()[0] is True
    p.inject()
    p.reference_mitigation()  # DEL: the next read refills the sets from the database
    assert p.fault_cleared()[0] is True and not any(key in k.zsets for key in p.scoped_keys)
    assert {c.read for c in p.write_checks()}


@pytest.mark.usefixtures("offline")
def test_a_scope_needs_a_keyspace_and_a_cold_cache_is_an_error_not_a_tiny_scope():
    k = _kube()
    with pytest.raises(ValueError, match="no wide scope"):
        _build("redis_poisoned_timeline_entry", _wide("x", "user-timeline-redis", poison="non_numeric", token="ab"), k)
    with pytest.raises(ValueError, match="do not pass key"):
        _build("redis_poisoned_timeline_entry",
               {**_wide("x", "home-timeline-redis", poison="non_numeric", token="ab"), "key": "9101"}, k)
    with pytest.raises(ValueError, match="single scope needs the key"):
        _build("redis_poisoned_timeline_entry", {"target": "home-timeline-redis", "poison": "non_numeric", "token": "ab"}, k)
    cold = ScriptKube()
    cold.zsets = {"9101": {"1": 1.0}, "3": {"1": 1.0}}
    p = _build("redis_timeline_key_renamed", _wide("x", "home-timeline-redis", token="a1b2c3"), cold)
    with pytest.raises(Exception, match="seeded keys match"):
        p.prepare_target()


def test_batch_scripts_stay_within_one_batch():
    with pytest.raises(ValueError):
        redis.eval_batch(None, "ns", None, redis.DELETE_ALL, tuple(str(i) for i in range(redis.MAX_SCOPED_KEYS + 1)))
    assert re.fullmatch(r".*", redis.backup_suffix("ab"))
    assert redis.backup_suffix("ab") == ":bak-ab"
