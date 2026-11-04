"""Pod-level faults (process freeze, netem): node seam, driver commands and per-target decoy rules.

A fake Kube serves pod specs and a fake node shell models one process (state, netem). Live
behavior is checked by tools/fault_validation/validate.py.
"""

from __future__ import annotations

import json
import random

import pytest

from sregym.conductor.problems.composition import app_bindings, catalog
from sregym.conductor.problems.composition.catalog import infrastructure
from sregym.conductor.problems.pod_network_impairment import PodNetworkImpairment
from sregym.conductor.problems.process_frozen import ProcessFrozen
from sregym.generators.fault.components import network, process
from sregym.generators.fault.components.kube import ComponentError
from sregym.generators.fault.components.node import Nodes
from tests.composition.test_component_faults import offline  # noqa: F401  (fixture)


class FakeKube:
    def __init__(self):
        self.calls: list[list[str]] = []
        self.thawed = False
        self.node_state = None  # set by FakeShell

    def json(self, argv):
        return json.loads(self.run([*argv, "-o", "json"]))

    def run(self, argv, stdin=None, *, timeout_s=None):
        self.calls.append(argv)
        if argv[:2] == ["get", "pod"]:
            return json.dumps(
                {
                    "spec": {"nodeName": "n1", "containers": [{"name": "c"}]},
                    "status": {
                        "containerStatuses": [
                            {"name": "c", "containerID": "containerd://abc", "state": {"running": {}}}
                        ]
                    },
                }
            ).encode()
        if argv[0] == "get" and argv[1] in ("deployment", "statefulset"):
            return json.dumps({"spec": {"selector": {"matchLabels": {"app": argv[2]}}}}).encode()
        if argv[:2] == ["get", "pods"]:
            return json.dumps(
                {"items": [{"metadata": {"name": "p-0", "creationTimestamp": "t"}, "status": {"phase": "Running"}}]}
            ).encode()
        if argv[0] == "exec":
            assert argv[-1] == "kill -CONT 1"
            self.node_state.state = "S"
            return b""
        if argv[:2] == ["rollout", "restart"]:
            self.node_state.state, self.node_state.netem = "S", False  # new pod, new process, new netns
            return b""
        if argv[:2] == ["rollout", "status"] or argv[0] == "delete":
            return b""
        raise AssertionError(argv)


class FakeShell:
    def __init__(self, kube):
        self.state, self.netem, self.cmds = "S", False, []
        kube.node_state = self

    def run(self, node, argv, *, timeout_s=60.0):
        assert node == "n1"
        self.cmds.append(argv)
        cmd = " ".join(argv)
        if argv[0] == "crictl":
            return "4242\n"
        if "kill -STOP 4242" in cmd:
            self.state = "T"
            return ""
        if "/proc/4242/status" in cmd:
            return f"State:\t{self.state} (x)\n"
        if "route show default" in cmd:
            return "default via 10.0.0.1 dev eth0\n"
        if "qdisc replace" in cmd:
            self.netem = True
            return ""
        if "qdisc del" in cmd:
            self.netem = False
            return ""
        if "qdisc show" in cmd:
            return "qdisc netem 8001: root delay 3s\n" if self.netem else "qdisc noqueue 0: root\n"
        raise AssertionError(cmd)


@pytest.fixture
def world(offline):  # noqa: F811
    kube = FakeKube()
    shell = FakeShell(kube)
    return kube, shell, Nodes(kube, shell)


def _frozen(world, target):
    kube, shell, nodes = world
    return ProcessFrozen("social_network", target, kube=kube, nodes=nodes), shell


def test_locate_uses_pod_spec_and_crictl(world):
    kube, shell, nodes = world
    ref = nodes.locate("ns", "p-0")
    assert (ref.node, ref.container, ref.container_id, ref.pid) == ("n1", "c", "abc", 4242)
    assert shell.cmds[0][:2] == ["crictl", "inspect"]


def test_freeze_then_reference_thaws(world):
    p, shell = _frozen(world, "user-service")
    assert p.fault_cleared()[0] is True
    p.inject()
    assert shell.state == "T" and p.fault_cleared()[0] is False
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True


def test_freeze_raises_when_signal_did_not_take(world):
    kube, shell, nodes = world
    shell.run = lambda node, argv, **k: (
        "4242\n" if argv[0] == "crictl" else ("State:\tS (x)\n" if "status" in " ".join(argv) else "")
    )
    with pytest.raises(ComponentError, match="after SIGSTOP"):
        process.freeze(nodes, "ns", "Deployment", "x", None)


def test_decoys_are_per_target(world):
    store, _ = _frozen(world, "home-timeline-redis")
    assert {"restore_source", "rollout_restart", "restart_stateful_pod"} == set(store.decoy_mitigations())
    assert not store.acceptable_mitigations()
    svc, _ = _frozen(world, "user-service")
    assert set(svc.decoy_mitigations()) == {"restore_source"}
    assert {"rollout_restart", "restart_stateful_pod"} == set(svc.acceptable_mitigations())
    cache, _ = _frozen(world, "user-timeline-redis")
    assert "rollout_restart" in cache.acceptable_mitigations()


def test_restart_clears_a_freeze_and_netem_on_stateless_target(world):
    kube, shell, nodes = world
    p = PodNetworkImpairment("social_network", "user-service", "delay", 3000, kube=kube, nodes=nodes)
    p.inject()
    assert p.fault_cleared()[0] is False
    p.acceptable_mitigations()["rollout_restart"]()
    assert p.fault_cleared()[0] is True


def test_netem_reference_removes_qdisc_and_validates_args(world):
    kube, shell, nodes = world
    p = PodNetworkImpairment("social_network", "home-timeline-redis", "loss", 60, kube=kube, nodes=nodes)
    p.inject()
    assert any("loss" in c and "60%" in c for c in map(" ".join, shell.cmds))
    p.reference_mitigation()
    assert shell.netem is False and p.fault_cleared()[0]
    with pytest.raises(ValueError):
        network.netem_args("jitter", 5)
    with pytest.raises(ValueError):
        network.netem_args("loss", 101)


def test_unknown_target_is_refused(offline):  # noqa: F811
    with pytest.raises(ValueError):
        ProcessFrozen("social_network", "nope")


def test_infrastructure_draws_declare_whole_workload_and_sample_every_target():
    by_key = {e.key: e for e in catalog.entries_for("social_network")}
    seen = set()
    for seed in range(200):
        draw = by_key["process_frozen"].sample("social_network", random.Random(seed))
        t = draw.param_dict()["target"]
        seen.add(t)
        assert [(f.kind, f.name, f.field) for f in draw.footprints] == [("Deployment", t, None)]
    assert seen == {
        t.workload for t in infrastructure._targets("social_network", protocols=("redis", "mongo", "thrift", "grpc"))
    }
    assert app_bindings.workload_targets(app_bindings.SOCIAL_NETWORK)


# --- foreign-pod faults (hog, surge) -----------------------------------------------------------------


def test_hog_script_opens_holds_and_reopens():
    from sregym.generators.fault.components import load

    s = load.hog_script("svc.ns.svc.cluster.local", 9090, 6000)
    assert "seq 6000" in s and "/dev/tcp/svc.ns.svc.cluster.local/9090" in s and "read -t 1 -u 10" in s


def test_foreign_pods_live_outside_the_app_namespace():
    from sregym.generators.fault.components import foreign

    m = foreign.pod_manifest("connection-hog", "social-network", "user-service", "img", "true", shell="bash")
    assert m["metadata"]["namespace"] == foreign.FOREIGN_NAMESPACE != "social-network"
    assert m["metadata"]["labels"]["sregym.io/app-namespace"] == "social-network"
    assert m["spec"]["containers"][0]["command"][0] == "bash"


def test_foreign_fault_clears_only_when_pod_is_gone(offline):  # noqa: F811
    from sregym.conductor.problems.connection_exhaustion import ConnectionExhaustion

    class K:
        present = True

        def run(self, argv, stdin=None, *, timeout_s=None):
            if argv[:2] == ["get", "pod"]:
                return b"pod/x" if self.present else b""
            if argv[0] == "delete":
                self.present = False
                return b""
            if argv[:2] == ["rollout", "restart"] or argv[:2] == ["rollout", "status"]:
                return b""
            raise AssertionError(argv)

    kube = K()
    p = ConnectionExhaustion("social_network", "unique-id-service", 6000, kube=kube)
    assert p.fault_cleared()[0] is False
    assert set(p.decoy_mitigations()) == {"restore_source", "rollout_restart"}
    p.decoy_mitigations()["rollout_restart"]()
    assert p.fault_cleared()[0] is False  # the hog outlives a restart of its victim
    p.reference_mitigation()
    assert p.fault_cleared()[0] is True
    with pytest.raises(ValueError):
        ConnectionExhaustion("social_network", "unique-id-service", 0, kube=kube)
