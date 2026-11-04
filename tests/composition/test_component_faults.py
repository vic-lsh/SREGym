"""Component-fault framework: bindings, app-neutral drivers and the per-fault contract.

The cluster is a fake ``Kube`` that interprets the client-pod scripts the drivers send: fetch
scripts against a tiny in-memory app (a timeline per user), memcached scripts against a dict,
and redis-cli execs against a role field. It checks the drivers' protocol handling and the
contract's verdict logic; live behavior is checked by tools/fault_validation/validate.py.
"""

from __future__ import annotations

import base64
import json
import random
import re
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from kubernetes.config import kube_config

from sregym.conductor.problems.composition import app_bindings, catalog
from sregym.conductor.problems.composition.catalog import component_state
from sregym.generators.fault.components import http, memcached, redis
from sregym.generators.fault.components.kube import ComponentError
from sregym.service.apps.base import Application
from sregym.service.kubectl import KubeCtl


class FakeKube:
    """Interprets kubectl argv. ``app`` maps path -> body for GETs; POSTs to compose append a post."""

    def __init__(self):
        self.timelines: dict[str, list[dict]] = {}  # user timelines
        self.homes: dict[str, list[dict]] = {}  # home timelines
        self.followers: dict[str, set[str]] = {}
        self.cache: dict[str, bytes] = {}
        self.role = "master"
        self.calls: list[list[str]] = []
        self.restarted: list[str] = []
        self._script = ""
        self.next_post = 1000

    def run(self, argv, stdin=None, *, timeout_s=None):
        self.calls.append(argv)
        if argv[:3] == ["apply", "-f", "-"]:
            self._script = json.loads(stdin)["spec"]["containers"][0]["command"][2]
            return b""
        if argv[0] == "get" and argv[1] == "pod":
            return b"Succeeded"
        if argv[0] == "logs":
            return self._interpret(self._script).encode()
        if argv[0] == "get" and argv[1] == "deployment":
            return json.dumps({"spec": {"selector": {"matchLabels": {"app": argv[2]}}}}).encode()
        if argv[0] == "get" and argv[1] == "pods":
            return json.dumps(
                {"items": [{"metadata": {"name": "p-0", "creationTimestamp": "t"}, "status": {"phase": "Running"}}]}
            ).encode()
        if argv[0] == "exec":
            cmd = argv[argv.index("--") + 1 :]
            if cmd[3:] == ["INFO", "replication"]:
                return f"# Replication\r\nrole:{self.role}\r\n".encode()
            if cmd[3] == "REPLICAOF":
                self.role = "master" if cmd[4:] == ["NO", "ONE"] else "slave"
                return b"OK\n"
        if argv[:2] == ["rollout", "restart"]:
            self.restarted.append(argv[2])
            return b""
        if argv[:2] == ["rollout", "status"] or argv[0] == "delete":
            return b""
        raise AssertionError(f"unexpected kubectl {argv}")

    def json(self, argv):
        return json.loads(self.run([*argv, "-o", "json"]))

    # --- script interpreters ---
    def _interpret(self, script: str) -> str:
        if "@@REQ" in script:
            return self._fetch(script)
        return self._memcached(script)

    def _fetch(self, script: str) -> str:
        out = []
        for i, part in enumerate(script.split("rm -f /tmp/body; ")[1:]):
            m = re.search(r"wget -q -T \d+ -O /tmp/body (?:--post-data (\S+) )?(\S+)", part)
            post = shlex.split(m.group(1))[0] if m.group(1) else None
            url = shlex.split(m.group(2))[0]
            path = url.split(":8080", 1)[1]
            ok, body = self._serve(path, post)
            out.append(f"@@REQ {i} {'OK' if ok else 'FAIL'} {base64.b64encode(body.encode()).decode()}")
        return "\n".join(out + ["@@END"])

    def _serve(self, path: str, post: str | None) -> tuple[bool, str]:
        if post is not None:
            fields = dict(p.split("=", 1) for p in post.split("&"))
            if path.endswith("/post/compose"):
                if self.role != "master":
                    return False, "READONLY"
                self.next_post += 1
                post = {"post_id": str(self.next_post), "text": fields["text"]}
                self.timelines.setdefault(fields["user_id"], []).insert(0, post)
                for f in self.followers.get(fields["user_id"], ()):
                    self.homes.setdefault(f, []).insert(0, post)
            elif path.endswith("/user/follow"):
                self.followers.setdefault(fields["followee_id"], set()).add(fields["user_id"])
            return True, "ok"
        m = re.search(r"user_id=(\d+)", path)
        posts = (self.homes if "home-timeline" in path else self.timelines).get(m.group(1), [])
        for p in posts:
            if self.cache.get(p["post_id"], b"").startswith(b"\xff"):
                return False, "parse error"
        return True, json.dumps([json.loads(self.cache.get(p["post_id"], json.dumps(p).encode())) for p in posts])

    def _memcached(self, script: str) -> str:
        out = []
        for m in re.finditer(r'echo "(@@R\d+) \$\(echo (\S+) \| base64 -d', script):
            payload = base64.b64decode(shlex.split(m.group(2))[0])
            cmd, _, rest = payload.partition(b"\r\n")
            words = cmd.split()
            if words[0] == b"get":
                v = self.cache.get(words[1].decode())
                reply = b"END\r\n" if v is None else b"VALUE %s 0 %d\r\n%s\r\nEND\r\n" % (words[1], len(v), v)
            elif words[0] == b"set":
                self.cache[words[1].decode()] = rest[: int(words[4])]
                reply = b"STORED\r\n"
            elif words[0] == b"delete":
                reply = b"DELETED\r\n" if self.cache.pop(words[1].decode(), None) is not None else b"NOT_FOUND\r\n"
            else:
                self.cache.clear()
                reply = b"OK\r\n"
            out.append(f"{m.group(1)} {base64.b64encode(reply).decode()}")
        return "\n".join(out + ["@@END"])


_KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters: [{name: offline, cluster: {server: "https://127.0.0.1:1"}}]
users: [{name: offline, user: {token: offline}}]
contexts: [{name: offline, context: {cluster: offline, user: offline}}]
current-context: offline
"""


@pytest.fixture
def offline(monkeypatch, tmp_path):
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(_KUBECONFIG)
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    monkeypatch.setattr(kube_config, "KUBE_CONFIG_DEFAULT_LOCATION", str(kubeconfig))
    monkeypatch.setattr(Application, "_validated_path", lambda self, path, label: Path(path).expanduser().resolve())
    monkeypatch.setattr(KubeCtl, "list_deployments", lambda self, namespace: SimpleNamespace(items=[]))
    monkeypatch.setattr(KubeCtl, "exec_command", lambda self, *args, **kwargs: "")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError(f"spawned {a}")))


# --- bindings and catalog ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", sorted(component_state.ENABLED_APPS))
def test_enabled_apps_have_verified_targets(key):
    for app in component_state.ENABLED_APPS[key]:
        b = app_bindings.BINDINGS[app]
        assert app_bindings.instances(b.redis if key.startswith("redis") else b.memcached)
        assert b.integrity is not None and b.integrity.reads and b.integrity.seed


def test_draws_sample_every_verified_target_and_declare_the_whole_workload():
    by_key = {e.key: e for e in catalog.entries_for("social_network")}
    seen = {key: set() for key in component_state.ENABLED_APPS}
    for seed in range(64):
        for key in seen:
            draw = by_key[key].sample("social_network", random.Random(seed))
            target = draw.param_dict()["target"]
            seen[key].add(target)
            assert [(f.kind, f.name, f.field) for f in draw.footprints] == [("Deployment", target, None)]
    b = app_bindings.SOCIAL_NETWORK
    assert seen["redis_replica_misconfig"] == {r.workload for r in b.redis}
    assert seen["memcached_poisoned_entries"] == {"user-memcached", "post-storage-memcached"}


def test_unverified_rows_are_not_sampled():
    assert not app_bindings.instances(app_bindings.ASTRONOMY_SHOP.redis)
    assert app_bindings.instances(app_bindings.ASTRONOMY_SHOP.redis, verified_only=False)


# --- pure driver pieces -----------------------------------------------------------------------------


def test_fetch_log_roundtrip_and_incomplete_log_raises():
    log = "noise\n@@REQ 1 FAIL \n@@REQ 0 OK " + base64.b64encode(b"[1]").decode() + "\n@@END"
    assert http.parse_fetch_log(log, 2) == [http.Response(True, "[1]"), http.Response(False, "")]
    with pytest.raises(ComponentError):
        http.parse_fetch_log("@@REQ 0 OK ", 1)


def test_diff_ignores_volatile_keys_and_reports_changes():
    snap = http.Snapshot(("/r",), (http.normalize('[{"id": 1, "t": 5}]', ("t",)),))
    assert http.diff(snap, [http.Response(True, '[{"id": 1, "t": 9}]')], ("t",))[0]
    ok, detail = http.diff(snap, [http.Response(True, "[]")], ("t",))
    assert not ok and "changed at /r" in detail
    assert not http.diff(snap, [http.Response(False, "")])[0]


def test_snapshot_json_field_values():
    snap = http.Snapshot(("/a", "/b"), ([{"post_id": "7", "x": {"post_id": 8}}], [{"post_id": "7"}]))
    assert snap.json_field_values("post_id") == ("7", "8")


@pytest.mark.parametrize("mode", memcached.CORRUPTION_MODES)
def test_corrupt_differs_and_is_deterministic(mode):
    value = b'{"password":"h","salt":"s","user_id":3}'
    a, b = memcached.corrupt(value, mode, 1), memcached.corrupt(value, mode, 1)
    assert a == b and a != value
    if mode == "garbage_bytes":
        with pytest.raises(UnicodeDecodeError):
            a.decode()
    elif mode == "wrong_type":
        assert json.loads(a) == {"password": ["h"], "salt": ["s"], "user_id": [3]}
    else:
        doc = json.loads(a)
        assert doc["user_id"] == 3 and doc["password"] != "h"


def test_parse_get_and_info():
    assert memcached.parse_get(b"END\r\n") is None
    assert memcached.parse_get(b"VALUE k 0 3\r\nabc\r\nEND\r\n") == b"abc"
    assert redis.parse_info("# Replication\r\nrole:slave\r\nmaster_host:x\r\n")["role"] == "slave"


# --- the contract on the two exemplars --------------------------------------------------------------


def _build(key, params, kube):
    entry = {e.key: e for e in catalog.entries_for("social_network")}[key]
    problem = entry.build(SimpleNamespace(app="social_network", param_dict=lambda: params))
    problem.kube = kube
    return problem


def test_capture_seeds_empty_reads_once(offline):
    kube = FakeKube()
    snap = http.capture(kube, app_bindings.SOCIAL_NETWORK, "ns")
    assert len(snap.values[0]) == 3  # three seeded posts by the author
    assert len(snap.values[1]) == 3  # and they reached the follower's home timeline
    again = http.capture(kube, app_bindings.SOCIAL_NETWORK, "ns")
    assert again == snap  # data present: no reseed


def test_redis_replica_reference_passes_and_restore_source_is_no_fix(offline):
    kube = FakeKube()
    p = _build("redis_replica_misconfig", {"target": "user-timeline-redis", "master_host": "redis-primary"}, kube)
    p.inject_fault()
    assert kube.role == "slave" and not p.verdict()["success"]
    assert set(p.decoy_mitigations()) == {"restore_source"}  # a cache over mongo: restart is not wrong
    p.reference_mitigation()
    assert p.verdict()["success"]


def test_redis_replica_on_primary_data_offers_rollout_restart_decoy(offline):
    kube = FakeKube()
    p = _build("redis_replica_misconfig", {"target": "home-timeline-redis", "master_host": "cache-master"}, kube)
    p.inject_fault()
    p.decoy_mitigations()["rollout_restart"]()
    assert kube.restarted == ["deployment/home-timeline-redis"]


@pytest.mark.parametrize("mode", memcached.CORRUPTION_MODES)
def test_memcached_poison_persists_until_keys_deleted(offline, mode):
    kube = FakeKube()
    params = {"target": "post-storage-memcached", "corruption": mode, "key_scope": "all_hot_keys", "poison_seed": 4}
    p = _build("memcached_poisoned_entries", params, kube)
    p.snapshot = http.capture(kube, p.binding, p.namespace)
    for post in kube.timelines["9100"]:  # the consumer cached each post on read
        kube.cache[post["post_id"]] = json.dumps(post).encode()
    p.inject()
    assert len(p.poisoned) == 3
    result = p.verdict()
    assert not result["success"] and not result["fault_cleared"]
    p.decoy_mitigations()["rollout_restart:post-storage-service"]()
    assert not p.verdict()["success"]
    p.reference_mitigation()
    assert p.verdict()["success"]


def test_memcached_poison_refuses_when_nothing_is_cached(offline):
    kube = FakeKube()
    params = {"target": "post-storage-memcached", "corruption": "stale_value", "key_scope": "one_key", "poison_seed": 1}
    p = _build("memcached_poisoned_entries", params, kube)
    p.snapshot = http.capture(kube, p.binding, p.namespace)
    with pytest.raises(ComponentError, match="none of the hot keys"):
        p.inject()
