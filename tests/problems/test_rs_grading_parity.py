"""Offline checks that these problems grade and describe their faults as robust-skills' SREGym does."""

from importlib import import_module
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.compound import CompoundedOracle
from sregym.conductor.oracles.deployment_readiness import DeploymentReadinessOracle
from sregym.conductor.oracles.incorrect_port import IncorrectPortAssignmentMitigationOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.oracles.mongo_auth_mitigation import MongoAuthMitigationOracle


class _App:
    namespace = "test-ns"

    def __init__(self, *args, **kwargs):
        self.mount_failure_scripts = True

    def create_workload(self):
        pass


_KUBECTL = SimpleNamespace(list_deployments=lambda namespace: SimpleNamespace(items=[]))


def _load(monkeypatch, module_name):
    module = import_module(f"sregym.conductor.problems.{module_name}")
    for name in ("HotelReservation", "SocialNetwork", "AstronomyShop", "TrainTicket"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, _App)
    if hasattr(module, "create_app"):
        monkeypatch.setattr(module, "create_app", lambda app_name: _App())
    for name in ("KubeCtl", "ApplicationFaultInjector", "VirtualizationFaultInjector"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *args, **kwargs: _KUBECTL)
    monkeypatch.setattr(module, "LLMAsAJudgeOracle", lambda **kwargs: SimpleNamespace(**kwargs))
    if hasattr(module, "client"):
        monkeypatch.setattr(
            module, "client", SimpleNamespace(AdmissionregistrationV1Api=lambda: None, CoreV1Api=lambda: None)
        )
    return module


@pytest.mark.parametrize(
    ("module_name", "class_name"),
    [("revoke_auth", "MongoDBRevokeAuth"), ("storage_user_unregistered", "MongoDBUserUnregistered")],
)
def test_mongo_auth_faults_probe_the_data_plane_and_blame_the_mongo_service(monkeypatch, module_name, class_name):
    cls = getattr(_load(monkeypatch, module_name), class_name)

    for problem in (
        cls(faulty_service="mongodb-rate"),
        cls(app_name="hotel_reservation", faulty_service="mongodb-rate"),
    ):
        assert isinstance(problem.mitigation_oracle, MongoAuthMitigationOracle)
        assert "component=service/mongodb-rate;" in problem.root_cause
        assert problem.app.mount_failure_scripts is False


def test_mongo_auth_oracle_passes_only_when_admin_can_read_the_service_db():
    commands = []

    def _oracle(output, service="mongodb-geo"):
        pods = SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=f"{service}-abc"))])
        kubectl = SimpleNamespace(list_pods=lambda ns: pods, exec_command=lambda cmd: commands.append(cmd) or output)
        problem = SimpleNamespace(kubectl=kubectl, namespace="hotel-reservation", faulty_service=service)
        return MongoAuthMitigationOracle(problem).evaluate()["success"]

    assert _oracle("PROBE_OK") is True
    assert "mongo geo-db -u admin -p admin" in commands[-1]
    assert _oracle("Error: not authorized on geo-db") is False
    assert _oracle("PROBE_OK", service="mongodb-user") is False


def test_duplicate_pvc_mounts_grades_with_mitigation_oracle(monkeypatch):
    cls = _load(monkeypatch, "duplicate_pvc_mounts").DuplicatePVCMounts
    problem = cls(app_name="social_network", faulty_service="jaeger")

    assert type(problem.mitigation_oracle) is MitigationOracle
    assert "multi-attach mount conflicts" in problem.root_cause


def test_incorrect_port_assignment_names_the_faulty_service_and_checks_the_env_port(monkeypatch):
    cls = _load(monkeypatch, "incorrect_port_assignment").IncorrectPortAssignment
    problem = cls(app_name="hotel_reservation", faulty_service="frontend", env_var="GEO_ADDR", incorrect_port=8083)

    assert isinstance(problem.mitigation_oracle, IncorrectPortAssignmentMitigationOracle)
    assert problem.incorrect_port == "8083"
    assert "so frontend cannot reach the dependent service" in problem.root_cause

    compound = cls(unschedulable=True).mitigation_oracle
    assert isinstance(compound, CompoundedOracle)


def test_target_port_description_says_the_port_moved_away_from_correct(monkeypatch):
    cls = _load(monkeypatch, "target_port").K8STargetPortMisconfig
    problem = cls(faulty_service="user-service", bad_port=9999)

    assert "misconfigured to 9999 (away from its correct port)" in problem.root_cause


@pytest.mark.parametrize(
    ("module_name", "class_name"),
    [
        ("admission_webhook_outage", "AdmissionWebhookOutage"),
        ("mutating_webhook_resource_limits", "MutatingWebhookResourceLimits"),
    ],
)
def test_webhook_faults_grade_with_deployment_readiness(monkeypatch, module_name, class_name):
    cls = getattr(_load(monkeypatch, module_name), class_name)
    if hasattr(cls, "APPS"):
        monkeypatch.setitem(cls.APPS, "hotel_reservation", _App)

    assert type(cls().mitigation_oracle) is DeploymentReadinessOracle


def test_mitigation_oracle_tolerates_pods_without_container_statuses():
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="geo-abc", deletion_timestamp=None),
        status=SimpleNamespace(phase="Running", container_statuses=None),
    )
    kubectl = SimpleNamespace(
        list_pods=lambda ns: SimpleNamespace(items=[pod]),
        list_deployments=lambda ns: SimpleNamespace(items=[]),
    )
    oracle = MitigationOracle(SimpleNamespace(kubectl=kubectl, namespace="hotel-reservation"))
    oracle.namespace_settled = True

    assert oracle.evaluate()["success"] is True
