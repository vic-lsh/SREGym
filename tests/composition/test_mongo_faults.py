"""MongoDB component faults: driver scripts, binding sampling and the per-fault contract.

The cluster is a fake ``Kube`` whose mongod is a small state machine that recognises the driver's
shell scripts. It checks the contract (symptom state, reference passes, each decoy fails because
the data is gone or the fault remains) and the draws; the scripts themselves were run against a
real mongo:4.4.6 (see docs/incident-faults.md) and are exercised live by
tools/fault_validation/validate.py.
"""

from __future__ import annotations

import json
import random
from types import SimpleNamespace

import pytest
from test_component_faults import FakeKube, offline  # noqa: F401  (offline is a fixture)

from sregym.conductor.problems import component_fault
from sregym.conductor.problems.component_fault import Expect
from sregym.conductor.problems.composition import app_bindings, catalog
from sregym.conductor.problems.composition.catalog import component_mongo
from sregym.generators.fault.components import mongo
from sregym.generators.fault.components.kube import ComponentError

pytestmark = pytest.mark.usefixtures("offline", "no_write_wait")


@pytest.fixture
def no_write_wait(monkeypatch):
    monkeypatch.setattr(component_fault, "WRITE_CHECK_WAIT_S", 0.0)


B = app_bindings.SOCIAL_NETWORK


class FakeMongoKube(FakeKube):
    """FakeKube plus a mongod behind ``exec`` of the shell and a Redis FLUSHALL."""

    def __init__(self):
        super().__init__()
        self.reset_data()
        self.pod_restarts = 0
        self.flushed = 0

    def reset_data(self):
        self.locked = False
        self.validator = False
        self.index = False
        self.sentinel = 0
        self.old_exists = False
        self.count = 4
        self.ids = ['ObjectId("a1")', 'ObjectId("a2")']
        self.poison: set[str] = set()
        self.missing: set[str] = set()

    def _serve(self, path, post):
        if post is not None and path.endswith("/post/compose") and (self.locked or self.validator or self.index):
            return True, "Successfully upload post"  # one-way compose: the app says 200, the write is lost
        return super()._serve(path, post)

    def wipe(self):
        self.reset_data()
        self.count, self.ids = 0, []
        self.pod_restarts += 1

    def run(self, argv, stdin=None, *, timeout_s=None):
        if argv[0] == "exec" and "--eval" in argv:
            self.calls.append(argv)
            return self._shell(argv[argv.index("--eval") + 1]).encode()
        if argv[0] == "exec" and "FLUSHALL" in argv:
            self.flushed += 1
            return b"OK\n"
        if argv[:2] == ["rollout", "restart"] and "mongodb" in argv[2]:
            self.wipe()  # no volume: the new pod is empty
        if argv[:2] == ["delete", "pod"] and "mongodb" in argv[2]:
            self.wipe()
        return super().run(argv, stdin, timeout_s=timeout_s)

    def _shell(self, s: str) -> str:
        def out(o):
            return "@@JSON " + json.dumps(o) + "\n"

        if "fsyncLock !== true){ chk(a.fsyncLock())" in s:
            self.locked = True
            return out({"locked": True})
        if "fsyncUnlock()" in s:
            self.locked = False
            return out({"locked": False, "unlocks": 1})
        if "out({locked: o.fsyncLock" in s:
            return out({"locked": self.locked})
        if 'validationLevel: "strict"' in s:
            self.validator = True
            return out({"ok": True})
        if 'validationLevel: "off"}' in s:
            self.validator = False
            return out({"ok": True})
        if "getCollectionInfos" in s:
            return out({"exists": True, "validator": self.validator, "level": "strict"})
        if "createIndexes" in s:
            self.index, self.sentinel = True, 1
            return out({"sentinel_id": "x", "threshold": 1})
        if "dropIndex" in s:
            self.index, self.sentinel = False, 0
            return out({"ok": True})
        if "sentinel: c.countDocuments" in s:
            return out({"index": self.index, "sentinel": self.sentinel})
        if "c.renameCollection(" in s and "_old" in s and "merged" not in s:
            self.old_exists = True
            return out({"ok": True})
        if "var merged" in s:
            merged = self.old_exists
            self.old_exists = False
            return out({"merged": 0, "renamed": merged})
        if "old_exists:" in s:
            return out({"exists": True, "count": 1, "old_exists": self.old_exists, "old_count": 3})
        if "var done = []" in s:
            keys = json.loads(s.split("var done = []; ")[1].split(".forEach")[0])
            self.poison |= {k for k in keys if k not in self.missing}
            return out({"done": [k for k in keys if k not in self.missing]})
        if "var p = [], m = []" in s:
            keys = json.loads(s.split("var p = [], m = []; ")[1].split(".forEach")[0])
            return out(
                {"poisoned": [k for k in keys if k in self.poison], "missing": [k for k in keys if k in self.missing]}
            )
        if "c.updateOne" in s and "forEach" in s:  # unpoison
            self.poison.clear()
            return out({"ok": True})
        if "sort({_id: 1})" in s:
            return out({"count": self.count, "ids": self.ids})
        if "present:" in s:
            ids = json.loads(s.split("var ids = ")[1].split(".map")[0])
            return out({"count": self.count, "present": len([i for i in ids if i in self.ids])})
        if "out({n: c.countDocuments" in s:
            return out({"n": self.count})
        raise AssertionError(f"unrecognised mongo script: {s[:300]}")


def _build(key, params, kube):
    entry = {e.key: e for e in catalog.entries_for("social_network")}[key]
    problem = entry.build(SimpleNamespace(app="social_network", param_dict=lambda: params))
    problem.kube = kube
    problem._key_params = (key, params)
    return problem


def _inject(key, params, kube):
    p = _build(key, params, kube)
    p.inject_fault()
    return p


# --- sampling -----------------------------------------------------------------------------------------


@pytest.mark.parametrize("key", sorted(component_mongo.ENABLED_APPS))
def test_draws_cover_every_verified_target_and_declare_the_whole_workload(key):
    entry = {e.key: e for e in catalog.entries_for("social_network")}[key]
    expected = {inst.workload for inst, _ in app_bindings.mongo_targets(B.mongo, key)}
    assert expected  # something is verified
    seen = set()
    for seed in range(64):
        draw = entry.sample("social_network", random.Random(seed))
        params = draw.param_dict()
        seen.add(params["target"])
        assert [(f.kind, f.name, f.field) for f in draw.footprints] == [("Deployment", params["target"], None)]
        assert "media-mongodb" not in params["target"]
    assert seen == expected


def test_unique_index_mode_only_where_the_collection_allows_it():
    entry = {e.key: e for e in catalog.entries_for("social_network")}["mongo_write_rejection"]
    modes = {}
    for seed in range(100):
        p = entry.sample("social_network", random.Random(seed)).param_dict()
        modes.setdefault(p["collection"], set()).add(p["mode"])
    assert all("validator" in m for m in modes.values())
    assert {c for c, m in modes.items() if "unique_index" in m} <= {"post"}


def test_unverified_instances_and_unlisted_faults_are_not_sampled():
    assert all(i.workload != "media-mongodb" for i, _ in app_bindings.mongo_targets(B.mongo, "mongo_write_lock"))
    assert not app_bindings.mongo_targets(B.mongo, "no_such_fault")


# --- driver error handling -------------------------------------------------------------------------------


def test_run_js_without_result_line_raises_with_context():
    class Silent(FakeMongoKube):
        def _shell(self, s):
            return "some banner\n"

    inst = B.mongo[0]
    with pytest.raises(ComponentError, match=r"user-timeline-mongodb in ns: no result from mongo"):
        mongo.run_js(Silent(), "ns", inst, "out({})")


def test_poison_shape_is_validated():
    with pytest.raises(ValueError):
        mongo.poison_documents(FakeMongoKube(), "ns", B.mongo[0], "c", "k", ("1",), "bogus", "a", "b")


# --- contract ----------------------------------------------------------------------------------------------


def _decoys_fail(p, kube, monkeypatch, tmp_path):
    """Each repair, on a fresh fake and injection, gives the verdict reason it declares."""
    key, params = p._key_params
    for name in sorted(p.repairs()):
        fresh = FakeMongoKube()
        fresh.followers["9200"] = {"9201"}
        q = _build(key, params, fresh)
        q.inject_fault()
        assert not q.verdict()["success"], f"{name}: verdict passed before any repair"
        repair = q.repairs()[name]
        q.apply(repair)
        v = q.verdict()
        assert v["reason"] == repair.expect.value, f"repair {name}: {v}"
        assert repair.expect is not Expect.PASSES, f"{name}: no repair but the reference is correct for mongo"


def test_write_lock_reference_passes_and_every_decoy_fails(monkeypatch, tmp_path):
    kube = FakeMongoKube()
    p = _inject("mongo_write_lock", {"target": "post-storage-mongodb"}, kube)
    assert kube.locked and not p.verdict()["success"]
    assert set(p.decoy_mitigations()) == {
        "restore_source",
        "rollout_restart",
        "restart_stateful_pod",
        "rollout_restart:post-storage-service",
    }
    assert not p.acceptable_mitigations()
    p.reference_mitigation()
    v = p.verdict()
    assert v["success"] and v["witness_ok"]
    _decoys_fail(p, kube, monkeypatch, tmp_path)


def test_restart_decoys_fail_through_the_data_check_not_the_fault_state():
    kube = FakeMongoKube()
    p = _inject("mongo_write_lock", {"target": "social-graph-mongodb"}, kube)
    p.apply(p.decoy_mitigations()["restart_stateful_pod"])
    v = p.verdict()
    assert v["fault_cleared"] and not v["witness_ok"] and v["reason"] == "data_lost"


def test_inject_refuses_an_empty_collection_after_seeding():
    kube = FakeMongoKube()
    kube.count, kube.ids = 0, []
    p = _build("mongo_write_lock", {"target": "post-storage-mongodb"}, kube)
    with pytest.raises(ComponentError, match="no documents to witness"):
        p.inject_fault()
    assert not kube.locked


@pytest.mark.parametrize("mode", ["validator", "unique_index"])
def test_write_rejection_reference_passes_and_decoys_fail(mode, monkeypatch, tmp_path):
    kube = FakeMongoKube()
    params = {"target": "post-storage-mongodb", "collection": "post", "mode": mode}
    p = _inject("mongo_write_rejection", params, kube)
    assert (kube.validator if mode == "validator" else kube.index) and not p.verdict()["success"]
    p.reference_mitigation()
    assert not kube.validator and not kube.index and kube.sentinel == 0
    assert p.verdict()["success"]
    _decoys_fail(p, kube, monkeypatch, tmp_path)


def test_unique_index_mode_needs_a_collection_that_supports_it():
    with pytest.raises(ValueError, match="unique_key"):
        _build(
            "mongo_write_rejection",
            {"target": "social-graph-mongodb", "collection": "social-graph", "mode": "unique_index"},
            FakeMongoKube(),
        )


@pytest.mark.parametrize(
    "target,collection",
    [("post-storage-mongodb", "post"), ("user-timeline-mongodb", "user-timeline")],
)
def test_renamed_collection_reference_merges_back_and_decoys_fail(target, collection, monkeypatch, tmp_path):
    kube = FakeMongoKube()
    p = _inject("mongo_collection_renamed", {"target": target, "collection": collection}, kube)
    assert kube.old_exists and not p.verdict()["success"]
    assert kube.flushed == (1 if collection == "user-timeline" else 0)  # a Redis cache is flushed; memcached via nc
    p.reference_mitigation()
    assert not kube.old_exists and p.verdict()["success"]
    _decoys_fail(p, kube, monkeypatch, tmp_path)


@pytest.mark.parametrize("shape", mongo.POISON_SHAPES)
@pytest.mark.parametrize("scope", ["one_doc", "all_seeded"])
def test_poison_document_reference_repairs_in_place_and_decoys_fail(shape, scope, monkeypatch, tmp_path):
    kube = FakeMongoKube()
    params = {"target": "post-storage-mongodb", "collection": "post", "shape": shape, "scope": scope}
    p = _build("mongo_poison_document", params, kube)
    p.inject_fault()
    assert len(p.poisoned) == (1 if scope == "one_doc" else 3)
    assert kube.poison and not p.verdict()["success"]
    assert "flush_cache" in p.decoy_mitigations()
    p.reference_mitigation()
    assert not kube.poison and p.verdict()["success"]
    _decoys_fail(p, kube, monkeypatch, tmp_path)


def test_poison_refuses_when_no_seeded_document_matches():
    kube = FakeMongoKube()
    params = {"target": "post-storage-mongodb", "collection": "post", "shape": "wrong_type", "scope": "one_doc"}
    p = _build("mongo_poison_document", params, kube)

    kube.missing = {"1001", "1002", "1003"}
    with pytest.raises(ComponentError, match="was poisoned"):
        p.inject_fault()


@pytest.mark.parametrize(
    "key,params",
    [
        ("mongo_write_lock", {"target": "post-storage-mongodb"}),
        (
            "mongo_write_rejection",
            {"target": "user-timeline-mongodb", "collection": "user-timeline", "mode": "validator"},
        ),
    ],
)
def test_write_faults_are_visible_through_the_write_round_trip(key, params):
    kube = FakeMongoKube()
    p = _inject(key, params, kube)
    v = p.verdict()
    assert "write" in v["failed"] and "not visible" in v["write_detail"]  # symptom visible, HTTP says 200
    p.reference_mitigation()
    v = p.verdict()
    assert v["success"] and "marker visible" in v["write_detail"]


def test_non_write_faults_skip_the_round_trip():
    kube = FakeMongoKube()
    p = _inject("mongo_collection_renamed", {"target": "post-storage-mongodb", "collection": "post"}, kube)
    assert "write_detail" not in p.verdict()
