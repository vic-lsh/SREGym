"""Cluster-free contracts for the fastloop playbook-program streams."""

import ast
import importlib
import inspect
import json
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from sregym.conductor.problems.registry import ProblemRegistry
from sregym.service.kubectl import KubeCtl

_STREAM_DIR = Path(__file__).resolve().parents[4] / "benchmarks/sregym/experiments/fastloop"
_STREAM_NAMES = ("pbprog_repeat", "pbprog_compose", "pbprog_explicit")
_PROBLEM_MODULES = (
    "target_port",
    "incorrect_image",
    "readiness_probe_misconfiguration",
    "wrong_service_selector",
    "network_policy_block",
)


def _problem_attributes(oracle) -> set[str]:
    source = ast.parse(textwrap.dedent(inspect.getsource(type(oracle))))
    return {
        node.attr
        for node in ast.walk(source)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "self"
        and node.value.attr == "problem"
    }


@pytest.fixture
def cluster_free_problems(monkeypatch):
    class App:
        namespace = "hotel-reservation"

        def create_workload(self):
            pass

    class DiagnosisOracle:
        def __init__(self, problem, expected):
            self.problem = problem
            self.expected = expected

    class Injector:
        def __init__(self, namespace):
            self.namespace = namespace

        def _inject(self, fault_type, microservices):
            pass

        def _recover(self, fault_type, microservices):
            pass

    monkeypatch.setattr(KubeCtl, "__init__", lambda self: None)
    monkeypatch.setattr(KubeCtl, "is_emulated_cluster", lambda self: False)
    monkeypatch.setattr(KubeCtl, "list_deployments", lambda self, namespace: SimpleNamespace(items=[]))
    monkeypatch.setattr(KubeCtl, "exec_command", lambda self, command: "")
    monkeypatch.setattr(KubeCtl, "wait_for_ready", lambda self, namespace, **kwargs: None)
    monkeypatch.setattr(
        KubeCtl,
        "get_service_json",
        lambda self, service, namespace: {"spec": {"ports": [{"port": 4000 + len(service)}]}},
    )
    for module_name in _PROBLEM_MODULES:
        module = importlib.import_module(f"sregym.conductor.problems.{module_name}")
        monkeypatch.setattr(module, "create_app", lambda app_name: App())
        monkeypatch.setattr(module, "LLMAsAJudgeOracle", DiagnosisOracle)
    selector = importlib.import_module("sregym.conductor.problems.wrong_service_selector")
    monkeypatch.setattr(selector, "VirtualizationFaultInjector", Injector)
    image = importlib.import_module("sregym.conductor.problems.incorrect_image")
    monkeypatch.setattr(image, "ApplicationFaultInjector", Injector)
    policy = importlib.import_module("sregym.conductor.problems.network_policy_block")
    monkeypatch.setattr(policy.client, "NetworkingV1Api", lambda: SimpleNamespace())
    return ProblemRegistry()


def test_stream_problem_oracles_have_their_problem_inputs(cluster_free_problems):
    if not all((_STREAM_DIR / f"{name}.json").exists() for name in _STREAM_NAMES):
        pytest.skip("fastloop stream manifests are available in the SDO parent worktree")

    incident_ids = set()
    for name in _STREAM_NAMES:
        stream = json.loads((_STREAM_DIR / f"{name}.json").read_text())
        for incident in stream["incidents"]:
            incident_ids.update(incident if isinstance(incident, list) else [incident])

    for problem_id in sorted(incident_ids):
        problem = cluster_free_problems.get_problem_instance(problem_id)
        if problem_id.startswith("wrong_service_selector"):
            problem.inject_fault()
        assert problem.diagnosis_oracle.expected == problem.root_cause, problem_id
        assert problem.root_cause.startswith("[fault_spec] "), problem_id
        missing = _problem_attributes(problem.mitigation_oracle) - vars(problem).keys()
        assert not missing, f"{problem_id}: mitigation oracle reads missing problem attributes {sorted(missing)}"


def test_every_hotel_selector_variant_captures_its_service_port(cluster_free_problems):
    problem_ids = sorted(
        problem_id
        for problem_id in cluster_free_problems.get_variant_ids(["wrong_service_selector"])
        if problem_id.startswith("wrong_service_selector__v_hotel_reservation_")
    )
    assert len(problem_ids) == 16

    for problem_id in problem_ids:
        problem = cluster_free_problems.get_problem_instance(problem_id)
        problem.inject_fault()
        assert problem.expected_service_port == 4000 + len(problem.faulty_service), problem_id
        problem.recover_fault()
        assert problem.expected_service_port == 4000 + len(problem.faulty_service), problem_id
