"""Offline tests for the missing-ConfigMap problem family (no cluster, no LLM)."""

from pathlib import Path

import pytest
import yaml

import sregym.conductor.problems.missing_configmap as missing_configmap_module
import sregym.conductor.problems.registry as registry_module
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.missing_configmap import MissingConfigMap
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.paths import TARGET_MICROSERVICES

HOTEL_VARIANTS = {
    "missing_configmap_hotel_reservation": ["mongodb-geo"],
    "missing_configmap_mongodb_rate_hotel_reservation": ["mongodb-rate"],
    "missing_configmap_mongodb_geo_rate_hotel_reservation": ["mongodb-geo", "mongodb-rate"],
}

_ORIGINAL_DESCRIPTION = (
    "A required ConfigMap for deployment `mongodb-geo` has been deleted, so pods lose required "
    "runtime configuration during startup and reload. Affected pods fail to initialize correctly or run "
    "with invalid defaults, leading to NotReady/CrashLoop behavior and unstable service operation. "
    "Users observe request failures and degraded functionality for features backed by this component."
)


class _App:
    def __init__(self):
        self.namespace = "hotel-reservation"
        self.workloads = 0

    def create_workload(self):
        self.workloads += 1


class _Oracle:
    def __init__(self, problem, expected=None):
        self.problem = problem
        self.expected = expected


class _KubeCtl:
    def __init__(self, configmaps=None):
        self.configmaps = configmaps or {}
        self.commands = []

    def exec_command(self, command):
        self.commands.append(command)
        if command.startswith("kubectl get configmap "):
            name = command.split()[3]
            return yaml.safe_dump(self.configmaps[name])
        return "ok"


class _Injector:
    instances = []

    def __init__(self, namespace):
        self.namespace = namespace
        self.calls = []
        self.__class__.instances.append(self)

    def _inject(self, fault_type, microservices):
        self.calls.append(("inject", fault_type, microservices))

    def _recover(self, fault_type, microservices):
        self.calls.append(("recover", fault_type, microservices))


@pytest.fixture
def offline_problem(monkeypatch):
    """Construct MissingConfigMap without a cluster: app, kubectl, and oracles are stubs."""
    monkeypatch.setattr(missing_configmap_module, "HotelReservation", _App)
    monkeypatch.setattr(missing_configmap_module, "KubeCtl", _KubeCtl)
    monkeypatch.setattr(missing_configmap_module, "LLMAsAJudgeOracle", _Oracle)
    monkeypatch.setattr(missing_configmap_module, "MitigationOracle", _Oracle)
    monkeypatch.setattr(missing_configmap_module, "VirtualizationFaultInjector", _Injector)
    _Injector.instances = []
    return MissingConfigMap


def _registry_factories(monkeypatch):
    class _SelectionKubeCtl:
        def is_emulated_cluster(self):
            return False

    monkeypatch.setattr(registry_module, "KubeCtl", _SelectionKubeCtl)
    return registry_module.ProblemRegistry().PROBLEM_REGISTRY


def _mounted_configmaps(manifest: Path) -> set[str]:
    deployment = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    volumes = deployment["spec"]["template"]["spec"].get("volumes", [])
    return {volume["configMap"]["name"] for volume in volumes if "configMap" in volume}


@pytest.mark.parametrize(("problem_id", "services"), sorted(HOTEL_VARIANTS.items()))
def test_registry_binds_each_variant_to_the_missing_configmap_class(monkeypatch, problem_id, services):
    constructed = []

    class _Recorder:
        def __init__(self, **kwargs):
            constructed.append(kwargs)

    factories = _registry_factories(monkeypatch)
    monkeypatch.setattr(registry_module, "MissingConfigMap", _Recorder)

    factories[problem_id]()

    expected = services[0] if len(services) == 1 else services
    assert constructed == [{"app_name": "hotel_reservation", "faulty_service": expected}]


@pytest.mark.parametrize("services", list(HOTEL_VARIANTS.values()))
def test_each_hotel_variant_targets_a_configmap_its_source_manifest_mounts(services):
    """The deleted ConfigMap must be a non-optional volume of the source-deployed workload."""
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.namespace = "hotel-reservation"
    manifests = {
        "mongodb-geo": "kubernetes/geo/mongodb-geo-deployment.yaml",
        "mongodb-rate": "kubernetes/rate/mongodb-rate-deployment.yaml",
    }
    for service in services:
        manifest = TARGET_MICROSERVICES / "hotelReservation" / manifests[service]
        if not manifest.is_file():
            pytest.skip("SREGym-applications submodule is not checked out")
        assert injector.required_configmap(service) in _mounted_configmaps(manifest)


def test_hotel_reservation_creates_every_required_configmap():
    from sregym.service.apps.hotel_reservation import HotelReservation

    created = []

    class _KubeCtlRecorder:
        def create_or_update_configmap(self, name, namespace, data):
            created.append(name)

    app = HotelReservation.__new__(HotelReservation)
    app.kubectl = _KubeCtlRecorder()
    app.namespace = "hotel-reservation"
    app._prepare_configmap_data = lambda files: {name: "" for name in files}

    app.create_configmaps()

    assert set(VirtualizationFaultInjector.REQUIRED_CONFIGMAPS["hotel-reservation"].values()) <= set(created)


def test_original_problem_keeps_its_ground_truth(offline_problem):
    problem = offline_problem(app_name="hotel_reservation", faulty_service="mongodb-geo")

    assert problem.faulty_service == "mongodb-geo"
    assert problem.faulty_services == ["mongodb-geo"]
    assert problem.root_cause == (
        f"[fault_spec] component=mongodb-geo; namespace=hotel-reservation || {_ORIGINAL_DESCRIPTION}"
    )
    assert problem.diagnosis_oracle.expected == problem.root_cause
    assert isinstance(problem.mitigation_oracle, _Oracle)
    assert problem.app.workloads == 1


def test_rate_variant_ground_truth_names_mongodb_rate(offline_problem):
    problem = offline_problem(app_name="hotel_reservation", faulty_service="mongodb-rate")

    assert problem.root_cause.startswith("[fault_spec] component=mongodb-rate; namespace=hotel-reservation || ")
    assert "deployment `mongodb-rate`" in problem.root_cause
    assert "mongodb-geo" not in problem.root_cause


def test_multi_workload_variant_ground_truth_names_both_deployments(offline_problem):
    problem = offline_problem(app_name="hotel_reservation", faulty_service=["mongodb-geo", "mongodb-rate"])

    assert problem.faulty_service == "mongodb-geo, mongodb-rate"
    assert "component=mongodb-geo, mongodb-rate;" in problem.root_cause
    assert "deployments `mongodb-geo` and `mongodb-rate`" in problem.root_cause
    assert "these components" in problem.root_cause


@pytest.mark.parametrize("services", list(HOTEL_VARIANTS.values()))
def test_inject_and_recover_cover_every_faulty_workload(offline_problem, services):
    problem = offline_problem(
        app_name="hotel_reservation", faulty_service=services if len(services) > 1 else services[0]
    )

    problem.inject_fault()
    assert problem.fault_injected is True
    problem.recover_fault()
    assert problem.fault_injected is False

    assert [instance.namespace for instance in _Injector.instances] == ["hotel-reservation"] * 2
    assert [call for instance in _Injector.instances for call in instance.calls] == [
        ("inject", "missing_configmap", services),
        ("recover", "missing_configmap", services),
    ]


@pytest.mark.parametrize("bad", [[], [""], ["mongodb-geo", "mongodb-geo"]])
def test_rejects_invalid_workload_lists(offline_problem, bad):
    with pytest.raises(ValueError):
        offline_problem(app_name="hotel_reservation", faulty_service=bad)


def test_problem_uses_the_generic_mitigation_and_judge_oracles():
    """The variants must be graded exactly like the original problem."""
    assert missing_configmap_module.MitigationOracle is MitigationOracle
    assert missing_configmap_module.LLMAsAJudgeOracle is LLMAsAJudgeOracle


def _injector(kubectl, namespace="hotel-reservation"):
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.namespace = namespace
    injector.kubectl = kubectl
    return injector


@pytest.mark.parametrize(
    ("namespace", "service", "configmap"),
    [
        ("hotel-reservation", "mongodb-geo", "mongo-geo-script"),
        ("hotel-reservation", "mongodb-rate", "mongo-rate-script"),
        ("social-network", "media-mongodb", "media-mongodb"),
        # Workloads without a known dependency keep the historical per-namespace default.
        ("hotel-reservation", "frontend", "mongo-geo-script"),
    ],
)
def test_required_configmap_resolution(namespace, service, configmap):
    assert _injector(_KubeCtl(), namespace).required_configmap(service) == configmap


def test_required_configmap_rejects_unknown_namespace():
    with pytest.raises(ValueError, match="Unknown namespace"):
        _injector(_KubeCtl(), "astronomy-shop").required_configmap("frontend")


def test_inject_deletes_the_rate_script_and_restarts_only_mongodb_rate(monkeypatch, tmp_path):
    written = {}
    kubectl = _KubeCtl(
        {
            "mongo-rate-script": {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "mongo-rate-script",
                    "namespace": "hotel-reservation",
                    "uid": "u-1",
                    "resourceVersion": "42",
                    "creationTimestamp": "2026-09-27T00:00:00Z",
                    "managedFields": [{"manager": "kubectl"}],
                    "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{}"},
                },
                "data": {"k8s-rate-mongo.sh": "echo rate"},
            }
        }
    )
    injector = _injector(kubectl)
    monkeypatch.setattr(injector, "_write_yaml_to_file", lambda service, content: written.update({service: content}))

    injector.inject_missing_configmap(["mongodb-rate"])

    assert kubectl.commands == [
        "kubectl get configmap mongo-rate-script -n hotel-reservation -o yaml",
        "kubectl delete configmap mongo-rate-script -n hotel-reservation",
        "kubectl scale deployment mongodb-rate -n hotel-reservation --replicas=0",
        "kubectl rollout status deployment mongodb-rate -n hotel-reservation --timeout=60s",
        "kubectl scale deployment mongodb-rate -n hotel-reservation --replicas=1",
    ]
    assert written == {
        "mongodb-rate": {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "mongo-rate-script", "namespace": "hotel-reservation"},
            "data": {"k8s-rate-mongo.sh": "echo rate"},
        }
    }


def test_inject_for_two_workloads_deletes_both_scripts():
    kubectl = _KubeCtl(
        {
            name: {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}, "data": {}}
            for name in ("mongo-geo-script", "mongo-rate-script")
        }
    )
    injector = _injector(kubectl)
    injector._write_yaml_to_file = lambda service, content: None

    injector.inject_missing_configmap(["mongodb-geo", "mongodb-rate"])

    deletes = [command for command in kubectl.commands if command.startswith("kubectl delete configmap")]
    assert deletes == [
        "kubectl delete configmap mongo-geo-script -n hotel-reservation",
        "kubectl delete configmap mongo-rate-script -n hotel-reservation",
    ]


def test_recover_reapplies_each_workload_backup_and_restarts_it():
    kubectl = _KubeCtl()

    _injector(kubectl).recover_missing_configmap(["mongodb-geo", "mongodb-rate"])

    assert kubectl.commands == [
        "kubectl apply -f /tmp/mongodb-geo_modified.yaml -n hotel-reservation",
        "kubectl rollout restart deployment mongodb-geo -n hotel-reservation",
        "kubectl rollout status deployment mongodb-geo -n hotel-reservation",
        "kubectl apply -f /tmp/mongodb-rate_modified.yaml -n hotel-reservation",
        "kubectl rollout restart deployment mongodb-rate -n hotel-reservation",
        "kubectl rollout status deployment mongodb-rate -n hotel-reservation",
    ]
