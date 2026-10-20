from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.target_port_mitigation import TargetPortMisconfigMitigationOracle
from sregym.conductor.problems.registry import ProblemRegistry
from sregym.conductor.problems.target_port import K8STargetPortMisconfig


@pytest.mark.parametrize(
    "service",
    ("frontend", "geo", "profile", "rate", "recommendation", "search", "reservation", "user"),
)
def test_hotel_target_port_variant_resolves_for_each_service(service):
    problem_id = f"k8s_target_port_misconfig_hotel_reservation__v_{service}_9999"

    problem = ProblemRegistry().get_problem_instance(problem_id)

    assert isinstance(problem, K8STargetPortMisconfig)
    assert (problem.app_name, problem.faulty_service, problem.bad_port) == ("hotel_reservation", service, 9999)


def test_existing_target_port_ids_still_resolve():
    registry = ProblemRegistry()

    assert registry.get_problem_instance("k8s_target_port_user_hotel_reservation").faulty_service == "user"
    assert (
        registry.get_problem_instance("k8s_target_port_misconfig__v_user-service_9999").faulty_service == "user-service"
    )


class _Kubectl:
    def __init__(self):
        self.target_port = 8080

    def get_service_json(self, service, namespace):
        return {"spec": {"ports": [{"targetPort": self.target_port}]}}

    def list_pods(self, namespace):
        return SimpleNamespace(items=[])


def test_target_port_oracle_uses_the_service_original_port():
    problem = SimpleNamespace(
        kubectl=_Kubectl(), namespace="hotel-reservation", faulty_service="user", original_port=8080
    )
    assert TargetPortMisconfigMitigationOracle(problem=problem).evaluate()["success"] is True


def test_target_port_fault_captures_and_restores_original_port(monkeypatch):
    kubectl = _Kubectl()
    calls = []

    class _Injector:
        def __init__(self, namespace):
            assert namespace == "hotel-reservation"

        def inject_misconfig_k8s(self, services, to_port, from_port):
            calls.append(("inject", services, to_port, from_port))

        def recover_misconfig_k8s(self, services, from_port, to_port):
            calls.append(("recover", services, from_port, to_port))

    monkeypatch.setattr("sregym.conductor.problems.target_port.VirtualizationFaultInjector", _Injector)
    problem = object.__new__(K8STargetPortMisconfig)
    problem.namespace = "hotel-reservation"
    problem.faulty_service = "user"
    problem.bad_port = 9999
    problem.kubectl = kubectl
    problem.inject_fault()
    problem.recover_fault()
    assert calls == [
        ("inject", ["user"], 9999, 8080),
        ("recover", ["user"], 9999, 8080),
    ]
