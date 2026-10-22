"""InternalTrafficPolicyMitigationOracle: per-node Running-pod coverage, then a probe (no cluster)."""

from types import SimpleNamespace

import pytest

from sregym.conductor.oracles import internal_traffic_policy_mitigation as module
from sregym.conductor.oracles.internal_traffic_policy_mitigation import InternalTrafficPolicyMitigationOracle

WORKERS = ["worker-a", "worker-b"]


def _pod(node, phase="Running"):
    return SimpleNamespace(spec=SimpleNamespace(node_name=node), status=SimpleNamespace(phase=phase))


class _CoreV1:
    def __init__(self, policy, pods):
        self.policy, self.pods = policy, pods
        self.pod_queries = []

    def read_namespaced_service(self, name, namespace):
        return SimpleNamespace(spec=SimpleNamespace(internal_traffic_policy=self.policy))

    def list_namespaced_pod(self, namespace, label_selector):
        self.pod_queries.append((namespace, label_selector))
        return SimpleNamespace(items=self.pods)


def _evaluate(monkeypatch, policy, pods, probe_ok=True):
    core = _CoreV1(policy, pods)
    monkeypatch.setattr(module.client, "CoreV1Api", lambda: core)
    problem = SimpleNamespace(
        namespace="astronomy-shop",
        FAULTY_SERVICE="recommendation",
        POD_LABEL_SELECTOR="app.kubernetes.io/name=recommendation",
        SERVICE_PORT=8080,
        victim_node="worker-b",
        worker_nodes=lambda: list(WORKERS),
    )
    oracle = InternalTrafficPolicyMitigationOracle(problem)
    probes = []
    oracle._connectivity_probe = lambda node: probes.append(node) or probe_ok
    return oracle.evaluate(), probes, core


def test_local_policy_with_an_uncovered_worker_fails_without_probing(monkeypatch):
    result, probes, core = _evaluate(monkeypatch, "Local", [_pod("worker-a")])
    assert result["success"] is False
    assert result["uncovered_nodes"] == ["worker-b"]
    assert probes == []
    assert core.pod_queries == [("astronomy-shop", "app.kubernetes.io/name=recommendation")]


def test_a_pending_pod_does_not_cover_its_node(monkeypatch):
    result, probes, _ = _evaluate(monkeypatch, "Local", [_pod("worker-a"), _pod("worker-b", "Pending")])
    assert result["success"] is False
    assert probes == []


def test_local_policy_with_a_running_pod_on_every_worker_is_judged_by_the_probe(monkeypatch):
    pods = [_pod("worker-a"), _pod("worker-b")]
    assert _evaluate(monkeypatch, "Local", pods)[0]["success"] is True
    result, probes, _ = _evaluate(monkeypatch, "Local", pods, probe_ok=False)
    assert result["success"] is False
    assert probes == ["worker-b"]  # the victim node


@pytest.mark.parametrize("policy", ["Cluster", None])
def test_cluster_policy_skips_coverage_and_probes(monkeypatch, policy):
    result, probes, _ = _evaluate(monkeypatch, policy, [_pod("worker-a")])
    assert result["success"] is True
    assert result["internalTrafficPolicy"] == "Cluster"
    assert probes == ["worker-b"]
