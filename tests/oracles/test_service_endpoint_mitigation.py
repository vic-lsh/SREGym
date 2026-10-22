"""ServiceEndpointMitigationOracle: every non-infra Service has a ready endpoint (no cluster)."""

from types import SimpleNamespace

from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.service_endpoint_mitigation import ServiceEndpointMitigationOracle


class _Kubectl:
    """`endpoints` maps a Service to its ready addresses, or to None when it has no Endpoints object."""

    def __init__(self, endpoints):
        self.endpoints = endpoints
        self.core_v1_api = self

    def list_services(self, namespace):
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in self.endpoints])

    def read_namespaced_endpoints(self, name, namespace):
        addresses = self.endpoints[name]
        if addresses is None:
            raise ApiException(status=404)
        return SimpleNamespace(subsets=[SimpleNamespace(addresses=addresses)] if addresses else None)


def _evaluate(endpoints):
    problem = SimpleNamespace(kubectl=_Kubectl(endpoints), namespace="hotel-reservation")
    return ServiceEndpointMitigationOracle(problem).evaluate()["success"]


def test_passes_when_every_service_has_a_ready_endpoint():
    assert _evaluate({"frontend": ["10.0.0.1"], "geo": ["10.0.0.2"]}) is True


def test_fails_when_an_application_service_has_no_ready_endpoint():
    assert _evaluate({"frontend": ["10.0.0.1"], "geo": []}) is False


def test_ignores_infrastructure_services():
    assert _evaluate({"frontend": ["10.0.0.1"], "jaeger": [], "prometheus-server": []}) is True


def test_skips_a_service_without_an_endpoints_object():
    assert _evaluate({"frontend": ["10.0.0.1"], "headless": None}) is True
