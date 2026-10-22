from types import SimpleNamespace

import pytest

from sregym.conductor.problems.namespace_memory_limit import NamespaceMemoryLimit


def _quota(name, hard):
    return SimpleNamespace(metadata=SimpleNamespace(name=name), spec=SimpleNamespace(hard=hard))


def _replica_set(name):
    return SimpleNamespace(metadata=SimpleNamespace(name=name))


class _KubeCtl:
    def __init__(self, *, quotas=None, replica_sets=None):
        self.quotas = list(quotas or [])
        self.replica_sets = (
            [_replica_set("search-current"), _replica_set("search-old")] if replica_sets is None else replica_sets
        )
        self.applied = []
        self.deleted_quotas = []
        self.deleted_replica_sets = []
        self.scales = []

    def apply_resource(self, body):
        self.applied.append(body)

    def get_matching_replicasets(self, namespace, deployment_name):
        assert deployment_name == "search"
        return self.replica_sets

    def delete_replicaset(self, name, namespace):
        self.deleted_replica_sets.append((name, namespace))

    def get_resource_quotas(self, namespace):
        return self.quotas

    def delete_resource_quota(self, name, namespace):
        self.deleted_quotas.append((name, namespace))

    def scale_deployment(self, name, namespace, replicas):
        self.scales.append((name, namespace, replicas))


def _problem(kubectl):
    problem = NamespaceMemoryLimit.__new__(NamespaceMemoryLimit)
    problem.kubectl = kubectl
    problem.namespace = "hotel-reservation"
    problem.faulty_service = "search"
    return problem


def test_injection_creates_the_quota_then_deletes_the_current_replica_set():
    # KubeCtl.get_matching_replicasets lists the current ReplicaSet first.
    kubectl = _KubeCtl()
    problem = _problem(kubectl)

    problem.inject_fault()

    assert kubectl.applied == [
        {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "memory-limit-quota", "namespace": "hotel-reservation"},
            "spec": {"hard": {"memory": "1Gi"}},
        }
    ]
    assert kubectl.deleted_replica_sets == [("search-current", "hotel-reservation")]
    assert problem.fault_injected is True


def test_injection_fails_without_a_replica_set():
    kubectl = _KubeCtl(replica_sets=[])

    with pytest.raises(RuntimeError, match="No ReplicaSet found"):
        _problem(kubectl).inject_fault()


def test_recovery_deletes_every_memory_quota_and_scales_search_to_one():
    kubectl = _KubeCtl(
        quotas=[
            _quota("memory-limit-quota", {"memory": "1Gi"}),
            _quota("agent-quota", {"memory": "4Gi", "pods": "30"}),
            _quota("unrelated-policy", {"pods": "30"}),
        ]
    )
    problem = _problem(kubectl)

    problem.recover_fault()

    assert kubectl.deleted_quotas == [
        ("memory-limit-quota", "hotel-reservation"),
        ("agent-quota", "hotel-reservation"),
    ]
    assert kubectl.scales == [("search", "hotel-reservation", 1)]
    assert problem.fault_injected is False
