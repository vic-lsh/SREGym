"""PSARestrictedBlocksRecreation finds its target pod under each app's labelling convention."""

from types import SimpleNamespace

import pytest

from sregym.conductor.problems.psa_restricted_blocks_recreation import PSARestrictedBlocksRecreation


def _labels_match(selector, labels):
    key, value = selector.split("=", 1)
    return labels.get(key) == value


class _CoreV1:
    def __init__(self, pods):
        self.pods = pods  # name -> labels
        self.deleted = []
        self.namespace_patches = []

    def list_namespaced_pod(self, namespace, label_selector):
        names = [name for name, labels in self.pods.items() if _labels_match(label_selector, labels)]
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in names])

    def delete_namespaced_pod(self, name, namespace, body):
        self.deleted.append((name, namespace))

    def read_namespace(self, namespace):
        return SimpleNamespace(metadata=SimpleNamespace(labels={}))

    def patch_namespace(self, namespace, body):
        self.namespace_patches.append(body)


def _problem(pods, namespace="astronomy-shop", faulty_service="checkout"):
    problem = PSARestrictedBlocksRecreation.__new__(PSARestrictedBlocksRecreation)
    problem.namespace = namespace
    problem.faulty_service = faulty_service
    problem.core_api = _CoreV1(pods)
    problem.fault_injected = False
    return problem


@pytest.mark.parametrize(
    "labels",
    [
        {"app.kubernetes.io/name": "checkout"},  # astronomy-shop (no io.kompose.service)
        {"io.kompose.service": "checkout"},  # hotel-reservation
        {"app": "checkout"},
        {"service": "checkout"},
    ],
)
def test_injection_deletes_the_target_pod_under_any_supported_label(labels):
    problem = _problem({"checkout-abc": labels, "cart-xyz": {"app.kubernetes.io/name": "cart"}})

    problem.inject_fault()

    assert problem.core_api.deleted == [("checkout-abc", "astronomy-shop")]
    assert problem.fault_injected is True


def test_missing_target_pod_raises():
    problem = _problem({"cart-xyz": {"app.kubernetes.io/name": "cart"}})

    with pytest.raises(RuntimeError, match="No pods found for service 'checkout'"):
        problem._find_target_pod()
