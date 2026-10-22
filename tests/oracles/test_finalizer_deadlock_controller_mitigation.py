from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.finalizer_deadlock_controller_mitigation import (
    _CONTROLLER_COMPLETION_ANNOTATION,
    FinalizerDeadlockControllerMitigationOracle,
)
from sregym.conductor.problems.finalizer_deadlock_controller import (
    _broken_clusterrole_rules,
    _correct_clusterrole_rules,
)

_CM = "reservation-cleanup-token"
_FINALIZER = "cleanup.reservations.io/pending-cleanup"


def _deployment(*, replicas=1, ready=1, completed=True):
    annotations = {_CONTROLLER_COMPLETION_ANNOTATION: "true"} if completed else {}
    return SimpleNamespace(
        metadata=SimpleNamespace(name="cleanup-controller", annotations=annotations),
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


def _pod(name, *, phase="Running", ready=True, container_statuses=...):
    if container_statuses is ...:
        container_statuses = [SimpleNamespace(name="c", ready=ready)]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(phase=phase, container_statuses=container_statuses),
    )


class _CoreV1:
    def __init__(self, configmap=None):
        self.configmap = configmap  # None: the ConfigMap is gone

    def read_namespaced_config_map(self, name, namespace, _request_timeout):
        if self.configmap is None:
            raise ApiException(status=404)
        return self.configmap


class _AppsV1:
    def __init__(self, deployment=None, read_error=None):
        self.deployment = deployment or _deployment()
        self.read_error = read_error

    def read_namespaced_deployment(self, name, namespace, _request_timeout):
        if self.read_error is not None:
            raise self.read_error
        return self.deployment


class _Rbac:
    def __init__(self, rules=None, read_error=None):
        self.rules = _correct_clusterrole_rules() if rules is None else rules
        self.read_error = read_error

    def read_cluster_role(self, name, _request_timeout):
        if self.read_error is not None:
            raise self.read_error
        return SimpleNamespace(rules=self.rules)


def _evaluate(*, core_v1=None, apps_v1=None, rbac=None, pods=None):
    pods = [_pod("frontend-abc"), _pod("cleanup-controller-abc", container_statuses=None)] if pods is None else pods
    kubectl = SimpleNamespace(
        core_v1_api=core_v1 or _CoreV1(),
        apps_v1_api=apps_v1 or _AppsV1(),
        list_pods=lambda namespace: SimpleNamespace(items=pods),
    )
    problem = SimpleNamespace(namespace="hotel-reservation", kubectl=kubectl)
    oracle = FinalizerDeadlockControllerMitigationOracle(
        problem=problem,
        configmap_name=_CM,
        finalizer=_FINALIZER,
        clusterrole_name="cleanup-controller-role",
        controller_deployment_name="cleanup-controller",
    )
    oracle.rbac_v1 = rbac or _Rbac()
    oracle.namespace_settled = True
    return oracle.evaluate()["success"]


def test_passes_when_rbac_restored_and_controller_completed_cleanup():
    assert _evaluate() is True


def test_rejects_manual_finalizer_removal_while_rbac_still_broken():
    assert _evaluate(rbac=_Rbac(rules=_broken_clusterrole_rules())) is False


def test_rejects_deleted_clusterrole():
    assert _evaluate(rbac=_Rbac(read_error=ApiException(status=404))) is False


def test_rejects_resource_names_restriction_excluding_the_configmap():
    rules = [
        SimpleNamespace(resources=["configmaps"], verbs=["patch", "update"], resource_names=["other"]),
        SimpleNamespace(resources=["configmaps/finalizers"], verbs=["patch", "update"], resource_names=None),
    ]
    assert _evaluate(rbac=_Rbac(rules=rules)) is False


def test_rejects_configmap_still_terminating():
    stuck = SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="now", finalizers=[_FINALIZER]))
    assert _evaluate(core_v1=_CoreV1(configmap=stuck)) is False


def test_rejects_cleanup_that_bypassed_the_controller():
    assert _evaluate(apps_v1=_AppsV1(deployment=_deployment(completed=False))) is False


@pytest.mark.parametrize(
    "apps_v1",
    [_AppsV1(deployment=_deployment(replicas=1, ready=0)), _AppsV1(read_error=ApiException(status=404))],
    ids=["not-ready", "deleted"],
)
def test_rejects_unhealthy_controller(apps_v1):
    assert _evaluate(apps_v1=apps_v1) is False


def test_rejects_unhealthy_application_pod():
    assert _evaluate(pods=[_pod("frontend-abc", phase="Pending", ready=False)]) is False


def test_correct_role_grants_the_configmap_and_finalizer_operations_used_by_controller():
    rules = _correct_clusterrole_rules()
    correct = rules[0]
    broken = _broken_clusterrole_rules()[0]

    assert correct.resources == ["configmaps"]
    assert set(correct.verbs) == {"get", "list", "watch", "patch", "update"}
    assert rules[1].resources == ["configmaps/finalizers"]
    assert set(rules[1].verbs) == {"patch", "update"}
    assert "patch" not in broken.verbs
