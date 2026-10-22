"""NetworkPolicyMitigationOracle passes iff the injected NetworkPolicy is gone (no cluster)."""

from types import SimpleNamespace

from kubernetes.client.rest import ApiException

from sregym.conductor.oracles import network_policy_oracle as module
from sregym.conductor.oracles.network_policy_oracle import NetworkPolicyMitigationOracle


class _NetworkingV1:
    def __init__(self, error_status=None):
        self.error_status = error_status
        self.reads = []

    def read_namespaced_network_policy(self, name, namespace):
        self.reads.append((name, namespace))
        if self.error_status is not None:
            raise ApiException(status=self.error_status)
        return SimpleNamespace(metadata=SimpleNamespace(name=name))


def _evaluate(monkeypatch, api, **problem_attrs):
    monkeypatch.setattr(module.client, "NetworkingV1Api", lambda: api)
    problem = SimpleNamespace(namespace="astronomy-shop", faulty_service="cart", **problem_attrs)
    return NetworkPolicyMitigationOracle(problem).evaluate()["success"]


def test_policy_still_present_fails(monkeypatch):
    api = _NetworkingV1()
    assert _evaluate(monkeypatch, api, policy_name="cart-ingress-lockdown") is False
    assert api.reads == [("cart-ingress-lockdown", "astronomy-shop")]


def test_deleted_policy_passes_on_any_app(monkeypatch):
    assert _evaluate(monkeypatch, _NetworkingV1(404), policy_name="cart-ingress-lockdown") is True


def test_other_api_errors_fail(monkeypatch):
    assert _evaluate(monkeypatch, _NetworkingV1(403), policy_name="cart-ingress-lockdown") is False


def test_default_policy_name_comes_from_the_faulty_service(monkeypatch):
    api = _NetworkingV1(404)
    assert _evaluate(monkeypatch, api) is True
    assert api.reads == [("deny-all-cart", "astronomy-shop")]
