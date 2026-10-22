"""WrongPodSelectionMitigationOracle: endpoints non-empty and all frontend pods (no cluster)."""

from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles import wrong_pod_selection_mitigation as module
from sregym.conductor.oracles.wrong_pod_selection_mitigation import WrongPodSelectionMitigationOracle

NS = "hotel-reservation"
LABELS = {"frontend-7d9c-abcde": "frontend", "frontend-7d9c-fghij": "frontend", "search-5f6b-klmno": "search"}


def _ref(pod):
    return SimpleNamespace(kind="Pod", name=pod)


class _Discovery:
    def __init__(self, endpoints, error=False):
        self.endpoints, self.error = endpoints, error
        self.selectors = []

    def list_namespaced_endpoint_slice(self, namespace, label_selector):
        self.selectors.append(label_selector)
        if self.error:
            raise ApiException(status=404)
        return SimpleNamespace(items=[SimpleNamespace(endpoints=self.endpoints)])


class _CoreV1:
    def __init__(self, fallback_pods=()):
        self.fallback_pods = fallback_pods

    def read_namespaced_pod(self, name, namespace):
        return SimpleNamespace(metadata=SimpleNamespace(labels={"io.kompose.service": LABELS[name]}))

    def read_namespaced_endpoints(self, name, namespace):
        addresses = [SimpleNamespace(target_ref=_ref(p)) for p in self.fallback_pods]
        return SimpleNamespace(subsets=[SimpleNamespace(addresses=addresses)])


def _endpoint(pod, ready=True):
    return SimpleNamespace(target_ref=_ref(pod), conditions=SimpleNamespace(ready=ready))


def _evaluate(monkeypatch, discovery, fallback_pods=()):
    monkeypatch.setattr(module.client, "DiscoveryV1Api", lambda: discovery)
    problem = SimpleNamespace(
        kubectl=SimpleNamespace(core_v1_api=_CoreV1(fallback_pods)),
        namespace=NS,
        frontend_service="frontend",
        expected_endpoint_pod_label="frontend",
    )
    return WrongPodSelectionMitigationOracle(problem).evaluate()["success"]


def test_frontend_only_endpoints_pass(monkeypatch):
    discovery = _Discovery([_endpoint("frontend-7d9c-abcde"), _endpoint("frontend-7d9c-fghij")])
    assert _evaluate(monkeypatch, discovery) is True
    assert discovery.selectors == ["kubernetes.io/service-name=frontend"]


def test_a_selected_search_pod_fails(monkeypatch):
    assert (
        _evaluate(monkeypatch, _Discovery([_endpoint("frontend-7d9c-abcde"), _endpoint("search-5f6b-klmno")])) is False
    )


def test_an_unready_search_endpoint_is_not_counted(monkeypatch):
    discovery = _Discovery([_endpoint("frontend-7d9c-abcde"), _endpoint("search-5f6b-klmno", ready=False)])
    assert _evaluate(monkeypatch, discovery) is True


@pytest.mark.parametrize("fallback", [(), ("search-5f6b-klmno",)])
def test_empty_or_wrong_fallback_endpoints_fail(monkeypatch, fallback):
    assert _evaluate(monkeypatch, _Discovery([], error=True), fallback) is False


def test_falls_back_to_the_endpoints_api(monkeypatch):
    assert _evaluate(monkeypatch, _Discovery([]), ("frontend-7d9c-abcde",)) is True
