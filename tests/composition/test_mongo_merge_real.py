"""The rename repair against a real mongod (mongo:4.4.6 in Docker; skipped when it is unavailable).

A user-timeline-like collection (one aggregate document per user holding an array) and a post-like one
(one document per event) are renamed away, written to by "writers", and repaired with each merge
strategy: the correct ones keep every write, the lossy ones lose some.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time
import uuid

import pytest

from sregym.conductor.problems.composition.app_bindings import MongoInstance
from sregym.generators.fault.components import mongo

IMAGE = "mongo:4.4.6"


def _have_image() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(not _have_image(), reason=f"docker image {IMAGE} not available")


@pytest.fixture
def mongod(monkeypatch):
    name = f"sgf-merge-{uuid.uuid4().hex[:8]}"
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name, IMAGE], check=True, capture_output=True)

    def shell(js: str) -> str:
        return subprocess.run(
            ["docker", "exec", name, "mongo", "--quiet", "--eval", js], capture_output=True, text=True
        ).stdout

    for _ in range(30):
        if "pong" in shell("print(db.runCommand({ping: 1}).ok ? \"pong\" : \"\")"):
            break
        time.sleep(1)

    def run_js(kube, namespace, inst, script):
        out = shell(mongo._PRELUDE + script)
        line = [ln for ln in out.splitlines() if ln.startswith("@@JSON ")]
        assert line, out
        return json.loads(line[-1][len("@@JSON ") :])

    monkeypatch.setattr(mongo, "run_js", run_js)
    try:
        yield shell
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


INST = MongoInstance("m", "db")


def _docs(shell, coll: str):
    out = shell(f"print(JSON.stringify(db.getSiblingDB('db').{coll}.find().sort({{_id:1}}).toArray()))")
    return json.loads(out.strip().splitlines()[-1])


def _setup_aggregate(shell):
    shell(
        "var c=db.getSiblingDB('db').tl;"
        "c.insertMany([{_id:1, posts:[{p:1},{p:2}]},{_id:2, posts:[{p:3}]}]);"
    )
    mongo.rename_away(None, "ns", INST, "tl")
    # writers recreate the name: user 1 gets a new post, user 3 appears
    shell("var c=db.getSiblingDB('db').tl; c.insertMany([{_id:1, posts:[{p:10}]},{_id:3, posts:[{p:11}]}]);")


def test_union_arrays_keeps_old_history_and_new_writes(mongod):
    _setup_aggregate(mongod)
    mongo.merge_back(None, "ns", INST, "tl", "union_arrays")
    docs = {d["_id"]: [p["p"] for p in d["posts"]] for d in _docs(mongod, "tl")}
    assert docs == {1: [1, 2, 10], 2: [3], 3: [11]}
    assert not mongo.collection_state(None, "ns", INST, "tl")["old_exists"]


def test_overwrite_loses_old_history_and_drop_target_loses_new_writes(mongod):
    _setup_aggregate(mongod)
    mongo.merge_back(None, "ns", INST, "tl", "overwrite")
    docs = {d["_id"]: [p["p"] for p in d["posts"]] for d in _docs(mongod, "tl")}
    assert docs[1] == [10]  # posts 1 and 2 are gone
    mongod("db.getSiblingDB('db').tl.drop()")
    _setup_aggregate(mongod)
    mongo.merge_back(None, "ns", INST, "tl", "drop_target")
    docs = {d["_id"]: [p["p"] for p in d["posts"]] for d in _docs(mongod, "tl")}
    assert docs == {1: [1, 2], 2: [3]}  # every write made during the fault is gone


def test_append_only_inserts_new_ids_and_keeps_old_documents(mongod):
    mongod("var c=db.getSiblingDB('db').post; c.insertMany([{_id:1,t:'a'},{_id:2,t:'b'}])")
    mongo.rename_away(None, "ns", INST, "post")
    mongod("var c=db.getSiblingDB('db').post; c.insertMany([{_id:3,t:'c'}])")
    mongo.merge_back(None, "ns", INST, "post", "append_only")
    assert [d["_id"] for d in _docs(mongod, "post")] == [1, 2, 3]


def test_unknown_strategy_is_rejected():
    with pytest.raises(ValueError):
        mongo.merge_back(None, "ns", INST, "tl", "bogus")
