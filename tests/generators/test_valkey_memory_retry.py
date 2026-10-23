from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import sregym.generators.fault.inject_app as module
from sregym.generators.fault.inject_app import ApplicationFaultInjector


def test_retry_reuses_only_active_flood_job(monkeypatch):
    job = SimpleNamespace(
        metadata=SimpleNamespace(deletion_timestamp=None),
        spec=SimpleNamespace(suspend=False),
        status=SimpleNamespace(active=1, succeeded=None, failed=None),
    )
    calls = []

    def create(**kwargs):
        calls.append(kwargs["body"])
        if len(calls) > 1:
            raise ApiException(status=409)

    api = SimpleNamespace(create_namespaced_job=create, read_namespaced_job=lambda **_: job)
    monkeypatch.setattr(module.client, "BatchV1Api", lambda: api)
    injector = object.__new__(ApplicationFaultInjector)
    injector.namespace = "astronomy-shop"
    injector.inject_valkey_memory_disruption()
    injector.inject_valkey_memory_disruption()
    container = calls[0]["spec"]["template"]["spec"]["containers"][0]
    assert "pip install" not in str(container["command"])
    assert container["resources"]["limits"]["cpu"] == "250m"


def test_retry_does_not_accept_terminal_job(monkeypatch):
    def create(**_):
        raise ApiException(status=409)

    api = SimpleNamespace(
        create_namespaced_job=create,
        read_namespaced_job=lambda **_: SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp=None),
            spec=SimpleNamespace(suspend=False),
            status=SimpleNamespace(active=None, succeeded=1, failed=None),
        ),
    )
    monkeypatch.setattr(module.client, "BatchV1Api", lambda: api)
    injector = object.__new__(ApplicationFaultInjector)
    injector.namespace = "astronomy-shop"
    with pytest.raises(RuntimeError, match="not active"):
        injector.inject_valkey_memory_disruption()
