"""Component-fault framework: bindings, app-neutral drivers and the per-fault contract.

The cluster is a fake ``Kube`` that interprets the client-pod scripts the drivers send: fetch
scripts against a tiny in-memory app (a timeline per user), memcached scripts against a dict,
and redis-cli execs against a role field. It checks the drivers' protocol handling and the
contract's verdict logic; live behavior is checked by tools/fault_validation/validate.py.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import random
import re
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from kubernetes.config import kube_config

from sregym.conductor.problems import component_fault
from sregym.conductor.problems.component_fault import Expect, classify
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
        self.pod_namespaces: list[str] = []  # namespace of every helper pod applied
        self.restarted: list[str] = []
        self._script = ""
        self.next_post = 1000
        self.gens: dict[str, int] = {}
        self.drift = False  # live objects differ from the render: restore_source rolls workloads

    def run(self, argv, stdin=None, *, timeout_s=None):
        self.calls.append(argv)
        if argv[:3] == ["apply", "-f", "-"]:
            obj = json.loads(stdin)
            if obj["kind"] == "Namespace":
                return b""
            self.pod_namespaces.append(obj["metadata"]["namespace"])
            self._script = obj["spec"]["containers"][0]["command"][2]
            return b""
        if argv[0] == "get" and argv[1] == "pod":
            return b"Succeeded"
        if argv[0] == "logs":
            return self._interpret(self._script).encode()
        if argv[0] == "get" and argv[1] == "deployment":
            return json.dumps({"spec": {"selector": {"matchLabels": {"app": argv[2]}}}}).encode()
        if argv[0] == "get" and argv[1] == "pods":
            name = argv[argv.index("-l") + 1].split("=", 1)[1] + "-0"
            return json.dumps(
                {"items": [{"metadata": {"name": name, "creationTimestamp": "t"}, "status": {"phase": "Running"}}]}
            ).encode()
        if argv[0] == "get" and argv[1] == "deployments,statefulsets":
            names = [r.workload for r in app_bindings.SOCIAL_NETWORK.rows()]
            items = [{"kind": "Deployment", "metadata": {"name": n, "generation": self.gens.get(n, 1)}} for n in names]
            return json.dumps({"items": items}).encode()
        if argv[0] == "exec":
            cmd = argv[argv.index("--") + 1 :]
            workload = argv[argv.index("--") - 1].rsplit("-", 1)[0]
            if cmd[0] == "env":  # the check channel: env REDISCLI_AUTH=.. redis-cli --user U -p PORT args
                cmd = [cmd[2], *cmd[5:]]
            if cmd[3] == "ACL":
                return b"OK\n" if cmd[4] == "SETUSER" else b"(integer) 1\n"
            if cmd[3:] == ["INFO", "replication"]:
                return f"# Replication\r\nrole:{self.role}\r\n".encode()
            if cmd[3] == "REPLICAOF":
                self.role = "master" if cmd[4:] == ["NO", "ONE"] else "slave"
                return b"OK\n"
            keys = sorted(self.homes) if workload == "home-timeline-redis" else []
            if cmd[3] == "SCAN":
                return ("0\n" + "".join(f"{k}\n" for k in keys)).encode()
            if cmd[3] == "DBSIZE":
                return f"{len(keys)}\n".encode()
            if cmd[3] == "EXISTS":
                return f"{len([k for k in cmd[4:] if k in keys])}\n".encode()
        if argv[:2] == ["rollout", "restart"]:
            self.restarted.append(argv[2])
            self._restart(argv[2].split("/", 1)[1])
            return b""
        if argv[:2] == ["delete", "pod"] and not argv[2].startswith("sregym-"):  # not a client pod
            self._restart(argv[2].rsplit("-", 1)[0])
            return b""
        if argv[0] == "apply" and "-n" in argv:  # restore_source
            if self.drift:
                for r in app_bindings.SOCIAL_NETWORK.rows():
                    self._restart(r.workload)
            return b""
        if argv[:2] == ["rollout", "status"] or argv[0] == "delete":
            return b""
        raise AssertionError(f"unexpected kubectl {argv}")

    def _restart(self, workload: str) -> None:
        """A new pod: in-memory state of that workload is gone."""
        self.gens[workload] = self.gens.get(workload, 1) + 1
        if workload.endswith("-redis"):
            self.role = "master"
            if workload == "home-timeline-redis":
                self.homes.clear()
        if workload.endswith("-memcached"):
            self.cache.clear()

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
            m = re.search(r"wget -q (?:-S )?-T \d+ -O /tmp/body (?:--post-data (\S+) )?(\S+)", part)
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
            elif path.endswith("/user/login"):  # user-service caches the login record
                self.cache.setdefault(f"{fields['username']}:login", b'{"password":"h","salt":"s","user_id":9200}')
            return True, "ok"
        m = re.search(r"user_id=(\d+)", path)
        if m is None:
            return True, "<html>index</html>"
        posts = (self.homes if "home-timeline" in path else self.timelines).get(m.group(1), [])
        for p in posts:
            if self.cache.get(p["post_id"], b"").startswith(b"\xff"):
                return False, "parse error"
            self.cache.setdefault(p["post_id"], json.dumps(p).encode())  # post-storage caches on read
        return True, json.dumps([json.loads(self.cache[p["post_id"]]) for p in posts])

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
            elif words[0] == b"lru_crawler":
                from urllib.parse import quote

                lines = [f"key={quote(k, safe='')} exp=-1 la=1 cas=1 fetch=no cls=1 size={len(v)}\n" for k, v in self.cache.items()]
                reply = "".join(lines).encode() + b"END\r\n"
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
    monkeypatch.setattr(http.time, "sleep", lambda s: None)
    monkeypatch.setattr(component_fault.time, "sleep", lambda s: None)
    render = tmp_path / "render.yaml"
    render.write_text("")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(render))
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError(f"spawned {a}")))


# --- bindings and catalog ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", sorted(component_state.ENABLED_APPS))
def test_enabled_apps_have_verified_targets(key):
    for app in component_state.ENABLED_APPS[key]:
        b = app_bindings.BINDINGS[app]
        assert app_bindings.instances(b.redis if key.startswith("redis") else b.memcached)
        assert b.integrity is not None and b.integrity.reads  # the seed is optional: hotel's services seed themselves


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


def test_diff_compares_unordered_keys_as_sets_but_still_sees_missing_items():
    snap = http.Snapshot(("/r",), (http.normalize('{"features": [{"id": "1"}, {"id": "2"}]}', (), ("features",)),))
    swapped = http.Response(True, '{"features": [{"id": "2"}, {"id": "1"}]}')
    assert http.diff(snap, [swapped], (), ("features",))[0]
    assert not http.diff(snap, [swapped], ())[0]  # ordered by default
    assert not http.diff(snap, [http.Response(True, '{"features": [{"id": "2"}]}')], (), ("features",))[0]


def test_memcached_key_template_formats_snapshot_values():
    assert (
        app_bindings.MemcachedInstance("m", ("c",), key_field="id", key_template="{}_cap").key_template.format(7)
        == "7_cap"
    )


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


def _kube() -> FakeKube:
    kube = FakeKube()
    kube.followers["9200"] = {"9201"}  # the probe's setup: 9201 follows 9200
    return kube


CASES = [
    *(
        ("redis_replica_misconfig", {"target": r.workload, "master": "127.0.0.1:1"})
        for r in app_bindings.SOCIAL_NETWORK.redis
    ),
    *(
        (
            "memcached_poisoned_entries",
            {"target": t, "corruption": mode, "key_scope": "all_hot_keys", "poison_seed": 3},
        )
        for t in ("user-memcached", "post-storage-memcached")
        for mode in memcached.CORRUPTION_MODES
    ),
]


@pytest.mark.parametrize("key,params", CASES)
def test_every_repair_produces_its_declared_outcome(offline, key, params):
    """The class rule: on a fresh injection, each repair's verdict reason is the one it declares,
    and the reference passes. A restart of a store holding the only copy must be DATA_LOST."""
    names = sorted(_build(key, params, _kube()).repairs())
    for name in [None, *names]:
        kube = _kube()
        p = _build(key, params, kube)
        p.inject_fault()
        assert not p.verdict()["success"], f"{name}: verdict passed with the fault live"
        if name is None:
            p.recover_fault()
            p.prepare_injection()  # recovery released the snapshot; re-take it on the healthy app
            assert p.verdict()["reason"] == "passes"
            continue
        repair = p.repairs()[name]
        p.apply(repair)
        v = p.verdict()
        assert v["reason"] == repair.expect.value, f"{name}: {v}"
        if name in ("rollout_restart", "restart_stateful_pod"):
            loses = p.target.restart_loses_data
            assert (repair.expect is Expect.DATA_LOST) == loses and repair.destroys_data == loses


def test_standard_repairs_follow_the_target_row(offline):
    home = _build("redis_replica_misconfig", {"target": "home-timeline-redis", "master": "127.0.0.1:1"}, _kube())
    cache = _build("redis_replica_misconfig", {"target": "user-timeline-redis", "master": "127.0.0.1:1"}, _kube())
    assert set(home.decoy_mitigations()) == {
        "restore_source",
        "rollout_restart",
        "restart_stateful_pod",
        "rollout_restart:home-timeline-service",
    }
    assert not home.acceptable_mitigations()
    assert set(cache.acceptable_mitigations()) == {"rollout_restart", "restart_stateful_pod"}


def test_a_no_op_repair_that_rolls_a_workload_is_an_error(offline):
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "social-graph-redis", "master": "127.0.0.1:1"}, kube)
    p.inject_fault()
    kube.drift = True
    with pytest.raises(ComponentError, match="declared a no-op changed workloads"):
        p.apply(p.repairs()["restore_source"])


def test_an_oracle_error_is_inconclusive_never_a_pass(offline, monkeypatch):
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "social-graph-redis", "master": "127.0.0.1:1"}, kube)
    p.inject_fault()
    monkeypatch.setattr(p, "fault_cleared", lambda: (_ for _ in ()).throw(ComponentError("exec flake")))
    r = p.mitigation_oracle.evaluate()
    assert r == {"success": False, "inconclusive": True, "reason": "inconclusive", "error": "exec flake"}


def test_classify_puts_a_live_fault_before_lost_data():
    assert classify([]) is Expect.PASSES
    assert classify(["fault", "data"]) is Expect.FAULT_NOT_CLEARED
    assert classify(["write"]) is Expect.FAULT_NOT_CLEARED
    assert classify(["witness"]) is Expect.DATA_LOST


@pytest.mark.parametrize(
    "out",
    [
        "READONLY You can't write against a read only replica.\n\n",
        "NOPERM this user has no permissions to run the 'set' command or its subcommand\n\n",
        "OOM command not allowed when used memory > 'maxmemory'.\n\n",
        "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\n\n",
        "LOADING Redis is loading the dataset in memory\n\n",
        "WRONGTYPE Operation against a key holding the wrong kind of value\n\n",
        "ERR unknown command `NOSUCHCMD`, with args beginning with: \n\n",
        "WRONGPASS invalid username-password pair or user is disabled.\n\n",
        "(error) READONLY You can't write against a read only replica.\n",
    ],
)
def test_redis_error_replies_raise(offline, out):
    """Lines as redis-cli 6.2.4 prints them without a tty (exit 0), checked in docker."""
    assert redis.error_reply(out) is not None
    kube = _kube()
    kube.run = lambda argv, stdin=None, **k: (  # type: ignore[method-assign]
        out.encode() if argv[0] == "exec" else FakeKube.run(kube, argv, stdin)
    )
    with pytest.raises(ComponentError, match=out.replace("(error) ", "").split()[0]):
        redis.cli(kube, "ns", app_bindings.SOCIAL_NETWORK.redis[0], "SET", "k", "v")


@pytest.mark.parametrize("out", ["OK\n", "(integer) 3\n", "role:master\n", '{"ERR": 1}\n', "Errored post\n", "\n"])
def test_redis_values_are_not_errors(out):
    assert redis.error_reply(out) is None


def test_injected_state_round_trips_for_a_new_process(offline):
    kube = _kube()
    params = {
        "target": "post-storage-memcached",
        "corruption": "garbage_bytes",
        "key_scope": "one_key",
        "poison_seed": 1,
    }
    p = _build("memcached_poisoned_entries", params, kube)
    p.inject_fault()
    state = json.loads(json.dumps(p.injected_state()))
    fresh = _build("memcached_poisoned_entries", params, kube)
    fresh.load_injected_state(state)
    assert fresh.poisoned == p.poisoned
    fresh.reference_mitigation()
    assert fresh.fault_cleared()[0]


def test_memcached_hot_keys_include_the_probe_reads(offline):
    kube = _kube()
    kube.timelines["1"] = [{"post_id": "77", "text": "probe-visible"}]
    params = {
        "target": "post-storage-memcached",
        "corruption": "stale_value",
        "key_scope": "all_hot_keys",
        "poison_seed": 1,
    }
    p = _build("memcached_poisoned_entries", params, kube)
    p.inject_fault()
    assert "77" in p.poisoned


def test_broad_scope_poisons_live_keys_beyond_the_probe_paths(offline):
    kube = _kube()
    kube.timelines["1"] = [{"post_id": "77", "text": "probe-visible"}]
    p = _build(
        "memcached_poisoned_entries",
        {"target": "post-storage-memcached", "corruption": "stale_value", "key_scope": "broad", "poison_seed": 1},
        kube,
    )
    p.prepare_injection()
    for i in range(250):  # more than one client-pod chunk, keys no probe path reads
        kube.cache[f"u{i}:k"] = json.dumps({"v": f"val{i}"}).encode()
    p.inject()
    assert "77" in p.poisoned and len(p.poisoned) >= 251
    assert all(kube.cache[k] == v for k, v in p.poisoned.items())
    assert not p.fault_cleared()[0]
    p.reference_mitigation()
    assert p.fault_cleared()[0]


def test_memcached_empty_key_lists_send_nothing(offline):
    kube = _kube()
    inst = app_bindings.SOCIAL_NETWORK.memcached[0]
    assert memcached.get_many(kube, "ns", inst, ()) == {}
    memcached.delete_many(kube, "ns", inst, ())


def test_memcached_poison_refuses_when_nothing_is_cached(offline):
    kube = _kube()
    params = {"target": "post-storage-memcached", "corruption": "stale_value", "key_scope": "one_key", "poison_seed": 1}
    p = _build("memcached_poisoned_entries", params, kube)
    p.prepare_injection()
    kube.cache.clear()
    kube._serve = lambda path, post: (True, "[]")  # type: ignore[method-assign]
    with pytest.raises(ComponentError, match="none of the hot keys"):
        p.inject()


def test_the_snapshot_is_kept_until_recovery_and_never_retaken(offline):
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "home-timeline-redis", "master": "127.0.0.1:1"}, kube)
    p.prepare_injection()
    prepared, witnesses = p.snapshot, p.witnesses
    kube.timelines.clear()  # another fault broke the reads: the injection must not re-read them
    kube.homes.clear()
    p.inject_fault()
    p.inject_fault()  # a composite retry
    assert p.snapshot is prepared and p.witnesses is witnesses and kube.role == "slave"
    p.recover_fault()
    assert p.snapshot is None and not p.witnesses


def test_integrity_is_optional_per_binding(offline, monkeypatch):
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "social-graph-redis", "master": "127.0.0.1:1"}, kube)
    monkeypatch.setattr(p, "binding", dataclasses.replace(p.binding, integrity=None))
    p.inject_fault()
    assert p.snapshot is None
    p.reference_mitigation()
    v = p.verdict()
    assert v["success"] and v["integrity_detail"] == "no integrity check in the binding"


def test_a_failed_recovery_still_releases_the_snapshot(offline, monkeypatch):
    """Regression (Redis family): a raising reference left _prepared set, so the next trial reused
    a stale snapshot."""
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "home-timeline-redis", "master": "127.0.0.1:1"}, kube)
    p.inject_fault()
    monkeypatch.setattr(p, "reference_mitigation", lambda: (_ for _ in ()).throw(ComponentError("exec flake")))
    p.recover_fault()  # the decorator logs a recovery error instead of raising
    assert p.snapshot is None and not p.witnesses and not p._prepared


def test_integrity_read_polls_so_a_settle_artefact_is_not_data_loss(offline, monkeypatch):
    """Regression (FR2/FP2): the first seeded read after a restart came back empty and was charged
    as DATA_LOST; a read that settles within the poll window passes, a persistent diff still fails."""
    kube = _kube()
    p = _build("redis_replica_misconfig", {"target": "home-timeline-redis", "master": "127.0.0.1:1"}, kube)
    p.inject_fault()
    p.reference_mitigation()
    answers = iter([(False, "empty"), (False, "empty"), (True, "same")])
    monkeypatch.setattr(http, "compare", lambda *a: next(answers))
    assert p.verdict()["integrity_ok"] is True
    calls = []
    monkeypatch.setattr(http, "compare", lambda *a: calls.append(1) or (False, "changed"))
    v = p.verdict()
    assert v["reason"] == "data_lost" and len(calls) == component_fault.INTEGRITY_ATTEMPTS


def test_classify_charges_lost_data_over_a_failing_write_once_the_fault_state_is_gone():
    from sregym.conductor.problems.component_fault import Expect, classify

    assert classify(["write", "witness"]) is Expect.DATA_LOST  # found live: restarted post-storage-mongodb
    assert classify(["fault", "write", "witness"]) is Expect.FAULT_NOT_CLEARED
    assert classify(["write"]) is Expect.FAULT_NOT_CLEARED
    assert classify([]) is Expect.PASSES


def test_a_blocked_write_is_retried_with_fresh_markers_and_a_persistent_block_still_fails():
    """A write right after a restart can fail once on a stale pooled connection (live: social_graph_stale_cache
    after restart_stateful_pod); a fault that blocks writes fails every try."""
    from sregym.conductor.problems.component_fault import WRITE_TRIES, ComponentFaultProblem

    class Bare(ComponentFaultProblem):
        inject = fault_cleared = reference_mitigation = lambda self: None

    p = object.__new__(Bare)
    outcomes = iter([False, False, True])
    written = []
    p._write_marker = lambda check: written.append(1) or f"m{len(written)}"
    p._marker_visible = lambda check, marker: (next(outcomes), f"saw {marker}")
    assert p._write_lands(object()) == (True, "saw m3") and len(written) == 3
    written.clear()
    p._marker_visible = lambda check, marker: (False, f"no {marker}")
    landed, detail = p._write_lands(object())
    assert not landed and len(written) == WRITE_TRIES and f"{WRITE_TRIES} writes" in detail


def _seeded_cache_kube(n_users=20):
    kube = _kube()
    for u in range(n_users):  # seeded users with a post each; the probe's user 9200 too
        kube.homes[str(u)] = []
        kube.timelines[str(u)] = [{"post_id": str(900 + u), "text": f"t{u}", "creator": {"user_id": u}}]
    kube.homes["9200"] = []
    kube.timelines["9200"] = [{"post_id": "990", "text": "probe", "creator": {"user_id": 9200}}]
    return kube


@pytest.mark.parametrize("scope,expected", [("wide", 20), ("partial", 10)])
def test_scoped_memcached_poison_chooses_seeded_users_entries_and_a_first_key_delete_is_a_decoy(offline, scope, expected):
    kube = _seeded_cache_kube()
    params = {"target": "post-storage-memcached", "corruption": "stale_value", "key_scope": scope, "poison_seed": 1,
              "pick_seed": 4, **({"fraction": 0.5} if scope == "partial" else {})}
    p = _build("memcached_poisoned_entries", params, kube)
    p.prepare_target()  # a cold cache is filled by reading as seeded users
    assert len(p.scoped_keys) == expected and "990" not in p.scoped_keys
    reads = p.extra_reads()
    assert 1 <= len(reads) <= 3 and all("user-timeline/read?user_id=" in r for r in reads)
    p.inject()
    assert set(p.poisoned) == set(p.scoped_keys) and not p.fault_cleared()[0]
    assert p.repairs()["delete_first_key"].expect is Expect.FAULT_NOT_CLEARED
    p.repairs()["delete_first_key"].run()
    assert not p.fault_cleared()[0]  # one repaired entry is not a repair of the incident
    p.reference_mitigation()
    assert p.fault_cleared()[0]


def test_scoped_memcached_needs_a_per_user_read(offline):
    params = {"target": "user-memcached", "corruption": "stale_value", "key_scope": "wide", "poison_seed": 1,
              "pick_seed": 1}
    with pytest.raises(ValueError, match="no wide scope"):
        _build("memcached_poisoned_entries", params, _kube())
