"""Offline wiring of EnvVariableShadowing (no cluster, no LLM)."""

import pytest

import sregym.conductor.problems.env_variable_shadowing as module
from sregym.conductor.problems.env_variable_shadowing import EnvVariableShadowing


class _App:
    def __init__(self, name):
        self.name = name
        self.namespace = "astronomy-shop"
        self.wrk = object()
        self.workloads = 0

    def create_workload(self):
        self.workloads += 1


class _Oracle:
    def __init__(self, problem, expected=None, wrk_manager=None):
        self.problem = problem
        self.expected = expected
        self.wrk_manager = wrk_manager


class _Injector:
    calls = []

    def __init__(self, namespace):
        self.namespace = namespace

    def inject_env_variable_shadowing(self, microservices):
        self.calls.append(("inject", self.namespace, microservices))

    def recover_env_variable_shadowing(self, microservices):
        self.calls.append(("recover", self.namespace, microservices))


@pytest.fixture
def problem(monkeypatch):
    monkeypatch.setattr(module, "create_app", _App)
    monkeypatch.setattr(module, "KubeCtl", lambda: None)
    monkeypatch.setattr(module, "LLMAsAJudgeOracle", _Oracle)
    monkeypatch.setattr(module, "WorkloadOracle", _Oracle)
    monkeypatch.setattr(module, "VirtualizationFaultInjector", _Injector)
    _Injector.calls = []
    return EnvVariableShadowing(app_name="astronomy_shop", faulty_service="frontend-proxy")


def test_catalog_constructor_builds_the_named_app_and_grades_with_the_workload(problem):
    assert problem.app.name == "astronomy_shop"
    assert problem.app.workloads == 1
    assert problem.mitigation_oracle.wrk_manager is problem.app.wrk
    assert problem.diagnosis_oracle.expected == problem.root_cause
    assert "deployment/frontend-proxy" in problem.root_cause
    assert "FRONTEND_HOST" in problem.root_cause


def test_inject_and_recover_go_through_the_virtualization_injector(problem):
    problem.inject_fault()
    assert problem.fault_injected is True
    problem.recover_fault()

    assert _Injector.calls == [
        ("inject", "astronomy-shop", ["frontend-proxy"]),
        ("recover", "astronomy-shop", ["frontend-proxy"]),
    ]
    assert problem.fault_injected is False
