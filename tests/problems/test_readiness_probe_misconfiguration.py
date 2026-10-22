import pytest

import sregym.conductor.problems.readiness_probe_misconfiguration as readiness_problem_module
import sregym.paths
from sregym.conductor.problems.readiness_probe_misconfiguration import ReadinessProbeMisconfiguration
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector


class _Injector:
    instances = []

    def __init__(self, namespace):
        self.namespace = namespace
        self.recoveries = []
        self.__class__.instances.append(self)

    def _recover(self, fault_type, microservices):
        self.recoveries.append((fault_type, microservices))


class _KubeCtl:
    def __init__(self):
        self.commands = []
        self.waited_namespaces = []

    def exec_command(self, command):
        self.commands.append(command)
        return "ok"

    def wait_for_ready(self, namespace):
        self.waited_namespaces.append(namespace)


def test_fresh_problem_recovery_constructs_its_own_injector(monkeypatch):
    _Injector.instances = []
    monkeypatch.setattr(readiness_problem_module, "VirtualizationFaultInjector", _Injector)
    problem = ReadinessProbeMisconfiguration.__new__(ReadinessProbeMisconfiguration)
    problem.namespace = "social-network"
    problem.faulty_service = "user-service"
    problem.fault_injected = True

    problem.recover_fault()

    assert len(_Injector.instances) == 1
    assert _Injector.instances[0].namespace == "social-network"
    assert _Injector.instances[0].recoveries == [
        ("readiness_probe_misconfiguration", ["user-service"]),
    ]
    assert problem.fault_injected is False


@pytest.fixture
def injector(tmp_path, monkeypatch):
    monkeypatch.setattr(sregym.paths, "FAULT_SCRATCH_ROOT", tmp_path)
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.namespace = "social-network"
    injector.kubectl = _KubeCtl()
    return injector


def test_recovery_restores_saved_manifest_when_deployment_is_missing(injector, tmp_path):
    injector._save_backup("readiness_probe_misconfiguration", "user-service", {"kind": "Deployment"})

    injector.recover_readiness_probe_misconfiguration(["user-service"])

    assert injector.kubectl.commands == [
        "kubectl delete deployment user-service -n social-network --ignore-not-found=true",
        f"kubectl apply -f {tmp_path}/social-network_user-service_readiness_probe_misconfiguration_backup.yaml"
        " -n social-network",
    ]
    assert injector.kubectl.waited_namespaces == ["social-network"]


def test_recovery_without_a_backup_skips_the_deployment(injector):
    injector.recover_readiness_probe_misconfiguration(["user-service"])

    assert injector.kubectl.commands == []


def test_recovery_continues_when_the_namespace_never_becomes_ready(injector):
    def never_ready(namespace):
        raise TimeoutError("another fault keeps a pod unready")

    injector.kubectl.wait_for_ready = never_ready
    injector._save_backup("readiness_probe_misconfiguration", "user-service", {"kind": "Deployment"})

    injector.recover_readiness_probe_misconfiguration(["user-service"])  # must not raise

    assert injector.kubectl.commands[-1].startswith("kubectl apply -f ")
