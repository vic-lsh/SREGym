from types import SimpleNamespace

from sregym.conductor.problems.missing_service import MissingService


class _Kubectl:
    def __init__(self, exists=True, ready=True):
        self.exists = exists
        self.ready = ready
        self.core_v1_api = self

    def list_services(self, namespace):
        names = ["mongodb-rate"] if self.exists else []
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=name)) for name in names])

    def read_namespaced_endpoints(self, name, namespace):
        addresses = [SimpleNamespace(ip="127.0.0.1")] if self.ready else []
        return SimpleNamespace(subsets=[SimpleNamespace(addresses=addresses)])


def test_missing_service_grading_requires_the_named_service_and_ready_endpoints():
    from sregym.conductor.oracles.missing_service_mitigation import MissingServiceMitigationOracle

    problem = object.__new__(MissingService)
    problem.namespace = "hotel-reservation"
    problem.faulty_service = "mongodb-rate"
    oracle = MissingServiceMitigationOracle(problem)

    problem.kubectl = _Kubectl(exists=False)
    assert oracle.evaluate()["success"] is False
    problem.kubectl = _Kubectl(ready=False)
    assert oracle.evaluate()["success"] is False
    problem.kubectl = _Kubectl()
    assert oracle.evaluate()["success"] is True
