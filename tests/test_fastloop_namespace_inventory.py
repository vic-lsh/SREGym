"""A warm lane's full recovery returns the app namespace to its pre-injection object inventory."""

import json
import shlex
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
import yaml

import sregym.paths
from sregym.conductor.problems.hpa_missing_effective_cpu_request import HPAMissingEffectiveCPURequest
from sregym.fastloop.created_objects import ClusterObject, inventory_drift
from sregym.fastloop.worker import FastloopWorker

NS = "hotel-reservation"
APP_BASELINE = frozenset(
    {
        ClusterObject("Namespace", NS),
        ClusterObject("Deployment", "frontend", NS),
        ClusterObject("Service", "frontend", NS),
        ClusterObject("ConfigMap", "kube-root-ca.crt", NS),
    }
)
FRONTEND = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {"name": "frontend", "namespace": NS},
    "spec": {
        "selector": {"matchLabels": {"io.kompose.service": "frontend"}},
        "template": {
            "spec": {
                "containers": [{"name": "frontend", "resources": {"requests": {"cpu": "100m"}, "limits": {"cpu": "1"}}}]
            }
        },
    },
}


@dataclass
class Namespace:
    """The app namespace as kubectl and the worker's inventory both see it."""

    objects: set[ClusterObject] = field(default_factory=lambda: set(APP_BASELINE))
    deployment: dict = field(default_factory=lambda: json.loads(json.dumps(FRONTEND)))
    deleted: list[ClusterObject] = field(default_factory=list)

    def list_objects(self, namespaces=frozenset()):
        return frozenset(obj for obj in self.objects if not obj.namespace or obj.namespace in namespaces)

    def delete(self, obj):
        self.deleted.append(obj)
        self.objects.discard(obj)

    def hpas(self):
        return {obj.name for obj in self.objects if obj.kind == "HorizontalPodAutoscaler"}


class Kubectl:
    """The kubectl subset HPAMissingEffectiveCPURequest runs, over ``Namespace``."""

    def __init__(self, ns: Namespace):
        self.ns = ns

    def exec_command(self, command: str) -> str:
        args = shlex.split(command)[1:]
        verb, kind = args[0], args[1]
        if verb == "get" and kind == "limitrange":
            return json.dumps({"items": []})
        if verb == "top":
            return "frontend-abc 1m 10Mi"
        if verb == "get" and kind == "deployment":
            return json.dumps(self.ns.deployment)
        if verb == "rollout":
            return 'deployment "frontend" successfully rolled out'
        if verb == "apply":
            with open(args[2]) as file:
                manifest = yaml.safe_load(file)
            if manifest["kind"] == "Deployment":
                self.ns.deployment = manifest
            else:
                self.ns.objects.add(ClusterObject(manifest["kind"], manifest["metadata"]["name"], NS))
            return f"{manifest['kind'].lower()} configured"
        if verb == "delete" and kind == "hpa":
            self.ns.objects.discard(ClusterObject("HorizontalPodAutoscaler", args[2], NS))
            return ""
        if verb == "get" and kind == "hpa" and "--ignore-not-found" in args:
            return f"horizontalpodautoscaler/{args[2]}" if args[2] in self.ns.hpas() else ""
        if verb == "get" and kind == "hpa":
            cpu = self.ns.deployment["spec"]["template"]["spec"]["containers"][0].get("resources", {})
            missing = "cpu" not in cpu.get("requests", {})
            condition = {
                "type": "ScalingActive",
                "status": "False" if missing else "True",
                "reason": "FailedGetResourceMetric" if missing else "ValidMetricFound",
                "message": "missing request for cpu in container frontend" if missing else "",
            }
            return json.dumps({"status": {"conditions": [condition]}})
        raise AssertionError(f"unexpected kubectl command: {command}")


def hpa_problem(ns: Namespace) -> HPAMissingEffectiveCPURequest:
    """The real fault, with kubectl and its metrics oracle pointed at the fixture namespace."""
    problem = object.__new__(HPAMissingEffectiveCPURequest)
    problem.app = SimpleNamespace(namespace=NS, name="Hotel Reservation")
    problem.namespace = NS
    problem.faulty_service = "frontend"
    problem.kubectl = Kubectl(ns)
    problem.mitigation_oracle = SimpleNamespace(record_metrics_baseline=lambda: None)
    return problem


def worker(ns: Namespace, factory) -> FastloopWorker:
    w = FastloopWorker(
        problem_factory=factory,
        deployer=lambda *_: None,
        health_probe=lambda _: {},
        cluster_factory=lambda *_: ("fake", "fake"),
        cluster_inventory=ns,
    )
    w.created_objects_timeout_s = 1.0
    w.created_objects_poll_s = 0.0
    w.inventory_settle_timeout_s = 0.0
    return w


@pytest.fixture(autouse=True)
def scratch(monkeypatch, tmp_path):
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    monkeypatch.setattr(sregym.paths, "FAULT_SCRATCH_ROOT", tmp_path)


def test_the_hpa_fault_leaves_no_hpa_when_the_baseline_had_none():
    ns = Namespace()
    w = worker(ns, lambda _: hpa_problem(ns))
    w.handle({"op": "inject", "problem_id": "hpa_missing_effective_cpu_request"})
    assert ns.hpas() == {"frontend-capacity"}

    result = w.handle({"op": "recover", "mode": "full"})

    assert result["success"] is True, result
    # recover_fault re-ensures the HPA; the worker removes what the injection created after it.
    assert ns.hpas() == set()
    assert ns.list_objects(frozenset({NS})) == APP_BASELINE
    assert ns.deployment["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"] == {"cpu": "100m"}
    w.handle({"op": "inject", "problem_id": "hpa_missing_effective_cpu_request"})


def test_the_hpa_fault_keeps_an_hpa_the_baseline_already_had():
    ns = Namespace()
    ns.objects.add(ClusterObject("HorizontalPodAutoscaler", "frontend-capacity", NS))
    w = worker(ns, lambda _: hpa_problem(ns))
    w.handle({"op": "inject", "problem_id": "hpa_missing_effective_cpu_request"})

    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    assert ns.hpas() == {"frontend-capacity"}
    assert ns.deleted == []


@dataclass
class DriftingProblem:
    """A fault whose recovery leaves the namespace changed outside the injection window."""

    ns: Namespace
    leave: ClusterObject | None = None
    remove: ClusterObject | None = None
    app: SimpleNamespace = field(default_factory=lambda: SimpleNamespace(namespace=NS, name="Hotel Reservation"))
    namespace: str = NS

    def inject_fault(self):
        if self.remove is not None:
            self.ns.objects.discard(self.remove)

    def recover_fault(self):
        if self.leave is not None:
            self.ns.objects.add(self.leave)


def test_an_object_added_after_injection_fails_recovery_loudly_and_blocks_the_next_row():
    ns = Namespace()
    leftover = ClusterObject("NetworkPolicy", "deny-all", NS)
    w = worker(ns, lambda _: DriftingProblem(ns, leave=leftover))
    w.handle({"op": "inject", "problem_id": "drift"})

    result = w.handle({"op": "recover", "mode": "full"})

    assert result["success"] is False
    error = result["errors"][-1]["error"]
    assert error.startswith("eval: app namespace inventory")
    assert "not in the pre-injection baseline: NetworkPolicy/hotel-reservation/deny-all" in error
    with pytest.raises(RuntimeError, match="recovery failed"):
        w.handle({"op": "inject", "problem_id": "drift"})
    # Once the namespace matches again, a retry clears the failure without re-running recover_fault.
    ns.objects.discard(leftover)
    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    w.handle({"op": "inject", "problem_id": "drift"})


def test_a_baseline_object_left_deleted_fails_recovery_loudly():
    ns = Namespace()
    service = ClusterObject("Service", "frontend", NS)
    w = worker(ns, lambda _: DriftingProblem(ns, remove=service))
    w.handle({"op": "inject", "problem_id": "drift"})

    result = w.handle({"op": "recover", "mode": "full"})

    assert result["success"] is False
    assert (
        "missing from the pre-injection baseline: Service/hotel-reservation/frontend" in result["errors"][-1]["error"]
    )


def test_other_namespaces_and_churning_kinds_are_not_inventoried():
    ns = Namespace()
    other = ClusterObject("HorizontalPodAutoscaler", "web", "another-app")
    ns.objects.add(other)
    w = worker(ns, lambda _: DriftingProblem(ns))
    w.handle({"op": "inject", "problem_id": "drift"})
    ns.objects.discard(other)

    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    with pytest.raises(ValueError, match="unsupported"):
        ClusterObject("Pod", "frontend-abc", NS)
    with pytest.raises(ValueError, match="namespaced"):
        ClusterObject("HorizontalPodAutoscaler", "frontend-capacity")
    with pytest.raises(ValueError, match="cluster-scoped"):
        ClusterObject("Namespace", NS, NS)


def test_inventory_drift_is_values_free_and_empty_when_equal():
    a = ClusterObject("ConfigMap", "a", NS)
    b = ClusterObject("ConfigMap", "b", NS)
    assert inventory_drift({a}, {a}) == ""
    assert inventory_drift({a}, {b}) == (
        "not in the pre-injection baseline: ConfigMap/hotel-reservation/b; "
        "missing from the pre-injection baseline: ConfigMap/hotel-reservation/a"
    )
