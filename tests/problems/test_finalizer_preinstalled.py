"""The preinstalled variant changes runtime data/RBAC only; real controller owns cleanup."""

import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace as NS
from urllib.error import HTTPError

import pytest
from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.problems.finalizer_deadlock_controller_preinstalled import (
    FinalizerDeadlockControllerPreinstalled,
)

ROOT = Path(__file__).resolve().parents[2] / "SREGym-applications/hotelReservation/optional/cleanup-controller"


class FakeAPI:
    """Stateful API: writes require controller RBAC and deletion honors finalizers."""

    def __init__(self):
        self.codec = client.ApiClient()
        objects = json.loads((ROOT / "resources.json").read_text())["items"]
        self.source = {obj["kind"]: obj for obj in objects}
        self.role = self.decode("V1ClusterRole", self.source["ClusterRole"])
        self.binding = self.decode("V1ClusterRoleBinding", self.source["ClusterRoleBinding"])
        self.deployment = self.decode("V1Deployment", self.source["Deployment"])
        self.deployment.status = client.V1DeploymentStatus(ready_replicas=1, available_replicas=1)
        self.token = None
        self.mutations = []
        self.logs = ""
        spec = importlib.util.spec_from_file_location("hotel_cleanup", ROOT / "controller.py")
        self.controller = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.controller)

    def decode(self, kind, data):
        return self.codec._ApiClient__deserialize(data, kind)

    def read_cluster_role(self, *args, **kwargs):
        return self.role

    def read_cluster_role_binding(self, *args, **kwargs):
        return self.binding

    def patch_cluster_role(self, name, body, **kwargs):
        self.mutations.append(("rules", copy.deepcopy(body)))
        self.role.rules = [self.decode("V1PolicyRule", r) for r in body["rules"]]

    def read_namespaced_service_account(self, *args, **kwargs):
        return client.V1ServiceAccount(metadata=client.V1ObjectMeta(name="cleanup-controller"))

    def read_namespaced_config_map(self, name, *args, **kwargs):
        if name == "cleanup-controller-script":
            return client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=name), data={"controller.py": (ROOT / "controller.py").read_text()}
            )
        self.tick()
        if self.token is None:
            raise ApiException(status=404)
        return self.token

    def create_namespaced_config_map(self, namespace, body, **kwargs):
        assert self.token is None
        self.token = body
        self.token.metadata.resource_version = "1"
        self.mutations.append(("token", body))

    def delete_namespaced_config_map(self, name, namespace, **kwargs):
        self.mutations.append(("delete-token", name))
        if self.token is None:
            raise ApiException(status=404)
        self.token.metadata.deletion_timestamp = "2026-10-25T00:00:00Z"
        if not self.token.metadata.finalizers:
            self.token = None

    def read_namespaced_deployment(self, *args, **kwargs):
        if self.deployment is None:
            raise ApiException(status=404)
        return self.deployment

    def list_namespaced_pod(self, *args, **kwargs):
        return NS(items=[NS(metadata=NS(name="cleanup-controller-abc"))])

    def read_namespaced_pod_log(self, **kwargs):
        self.tick()
        return self.logs

    def list_pods(self, namespace):
        return NS(
            items=[
                NS(metadata=NS(name="frontend-abc"), status=NS(phase="Running", container_statuses=[NS(ready=True)]))
            ]
        )

    def tick(self):
        if self.deployment is None:
            return
        try:
            self.controller.reconcile(self.controller_request, "hotel-reservation")
        except HTTPError as exc:
            assert exc.code == 403
            self.logs = "Kubernetes API denied the finalizer cleanup request with Forbidden"

    def controller_request(self, path, method="GET", body=None):
        if "configmaps/" in path:
            if self.token is None:
                raise HTTPError(path, 404, "Not Found", {}, None)
            if method == "GET":
                return self.codec.sanitize_for_serialization(self.token)
            granted = any("configmaps" in rule.resources and "patch" in rule.verbs for rule in self.role.rules)
            if not granted:
                raise HTTPError(path, 403, "Forbidden", {}, None)
            self.token.metadata.finalizers = body["metadata"]["finalizers"]
            if self.token.metadata.deletion_timestamp and not self.token.metadata.finalizers:
                self.token = None
        else:
            self.deployment.metadata.annotations = body["metadata"]["annotations"]
        return {}


@pytest.fixture
def problem(monkeypatch):
    api = FakeAPI()
    legacy = __import__("sregym.conductor.problems.finalizer_deadlock_controller", fromlist=["x"])
    monkeypatch.setattr(
        legacy, "HotelReservation", lambda: NS(namespace="hotel-reservation", create_workload=lambda: None)
    )
    monkeypatch.setattr(legacy, "KubeCtl", lambda: NS(core_v1_api=api, apps_v1_api=api, list_pods=api.list_pods))
    monkeypatch.setattr(legacy, "LLMAsAJudgeOracle", lambda **kwargs: None)
    monkeypatch.setattr(client, "RbacAuthorizationV1Api", lambda: api)
    p = FinalizerDeadlockControllerPreinstalled()
    p.mitigation_oracle.namespace_settled = True
    return p, api


def test_source_reconcile_allows_real_controller_to_pass_unchanged_oracle(problem):
    p, api = problem
    p.inject_fault()
    assert [m[0] for m in api.mutations] == ["token", "rules", "delete-token"]
    assert api.token.metadata.labels["platform.io/injected"] == "true"
    assert not p.mitigation_oracle.evaluate()["success"]
    # Scripted responder restores exactly the accepted source rules.
    api.patch_cluster_role(p.clusterrole_name, {"rules": api.source["ClusterRole"]["rules"]})
    api.tick()
    assert p.mitigation_oracle.evaluate()["success"]


def test_deleting_source_controller_fails_oracle_and_leaves_source_absent(problem):
    p, api = problem
    p.inject_fault()
    api.deployment = None
    api.patch_cluster_role(p.clusterrole_name, {"rules": api.source["ClusterRole"]["rules"]})
    api.tick()
    assert not p.mitigation_oracle.evaluate()["success"]
    assert api.source["Deployment"]["metadata"]["name"] == p.controller_name
    with pytest.raises(ApiException):
        api.read_namespaced_deployment(p.controller_name)
    assert api.token is not None


@pytest.mark.parametrize("bad", ["rbac", "ready", "binding", "injected", "missing"])
def test_setup_rejects_unhealthy_or_non_source_controller_without_writes(problem, bad):
    p, api = problem
    if bad == "rbac":
        api.role.rules = []
    elif bad == "ready":
        api.deployment.status.ready_replicas = 0
    elif bad == "binding":
        api.binding.subjects[0].namespace = "wrong"
    elif bad == "injected":
        api.deployment.metadata.labels["platform.io/injected"] = "true"
    else:
        api.deployment = None
    with pytest.raises((RuntimeError, ApiException)):
        p.inject_fault()
    assert api.mutations == []


def test_recovery_preserves_source_objects(problem):
    p, api = problem
    p.inject_fault()
    api.mutations.clear()
    p.recover_fault.__wrapped__(p)
    assert api.deployment is not None and api.binding is not None
    assert all(m[0] in {"rules", "delete-token"} for m in api.mutations)


def test_absent_forbidden_log_fails_injection(problem):
    p, api = problem
    api.read_namespaced_pod_log = lambda **kwargs: ""
    with pytest.raises(TimeoutError):
        p._wait_for_forbidden_log(timeout_seconds=0)


def test_new_named_slot_is_explicit_and_preserves_the_other_draws(monkeypatch):
    from sregym.conductor.problems import registry
    from sregym.conductor.problems.composition import catalog
    from sregym.conductor.problems.composition.generator import PlanRef, plan_for
    from sregym.conductor.problems.finalizer_deadlock_controller_preinstalled import preinstalled_slot11_plan

    monkeypatch.setattr(registry, "KubeCtl", lambda: NS(is_emulated_cluster=lambda: False))
    registered = registry.ProblemRegistry().PROBLEM_REGISTRY
    baseline = plan_for(PlanRef("hotel_reservation", 3, 1276337490, reliable=True), catalog.entries())
    variant = preinstalled_slot11_plan()
    assert variant.draws[1:] == baseline.draws[1:]
    assert variant.draws[0].key == "finalizer_deadlock_controller_preinstalled"
    assert variant.draws[0].params == ()
    assert (
        registered["finalizer_deadlock_controller_preinstalled_hotel_reservation"]
        is FinalizerDeadlockControllerPreinstalled
    )
    assert "composite_slot11_preinstalled_hotel_reservation" in registered
    assert "finalizer_deadlock_controller_hotel_reservation" in registered
