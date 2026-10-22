from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.namespace_memory_limit_mitigation import (
    NamespaceMemoryLimitMitigationOracle,
)


def _quota(hard):
    return SimpleNamespace(metadata=SimpleNamespace(name="q"), spec=SimpleNamespace(hard=hard))


def _pod(name, *, phase="Running", ready=True, container_statuses=...):
    if container_statuses is ...:
        container_statuses = [SimpleNamespace(ready=ready)]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(phase=phase, container_statuses=container_statuses),
    )


def _evaluate(quotas, pods):
    kubectl = SimpleNamespace(
        get_resource_quotas=lambda namespace: quotas,
        list_pods=lambda namespace: SimpleNamespace(items=pods),
    )
    problem = SimpleNamespace(kubectl=kubectl, namespace="hotel-reservation", faulty_service="search")
    return NamespaceMemoryLimitMitigationOracle(problem).evaluate()["success"]


def test_passes_once_the_memory_quota_is_gone_and_a_search_pod_is_ready():
    pods = [_pod("search-old", phase="Pending", ready=False), _pod("search-abc")]
    assert _evaluate([_quota({"pods": "30"})], pods) is True


def test_fails_while_a_memory_quota_remains():
    assert _evaluate([_quota({"memory": "4Gi"})], [_pod("search-abc")]) is False


@pytest.mark.parametrize(
    "pods",
    [
        [],
        [_pod("geo-abc")],
        [_pod("search-abc", phase="Pending", ready=False)],
        [_pod("search-abc", ready=False)],
    ],
    ids=["no-pods", "other-service", "pending", "not-ready"],
)
def test_fails_without_a_ready_search_pod(pods):
    assert _evaluate([], pods) is False


def test_running_pod_without_container_statuses_does_not_crash():
    assert _evaluate([], [_pod("search-abc", container_statuses=None)]) is True
