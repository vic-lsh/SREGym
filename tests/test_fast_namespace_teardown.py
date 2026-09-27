"""Opt-in fast namespace teardown: skip the pods' termination grace period."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sregym.service.kubectl import KubeCtl


def _kubectl() -> tuple[KubeCtl, MagicMock]:
    kubectl = KubeCtl.__new__(KubeCtl)
    core = MagicMock()
    core.read_namespace.side_effect = Exception("not found")
    kubectl.core_v1_api = core
    return kubectl, core


def test_default_namespace_deletion_keeps_pod_grace_periods(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SREGYM_FAST_NAMESPACE_TEARDOWN", raising=False)
    kubectl, core = _kubectl()

    kubectl.delete_namespace("hotel-reservation")

    core.delete_namespace.assert_called_once_with(name="hotel-reservation")
    core.delete_collection_namespaced_pod.assert_not_called()


def test_fast_teardown_deletes_the_namespace_first_then_its_pods_without_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SREGYM_FAST_NAMESPACE_TEARDOWN", "1")
    kubectl, core = _kubectl()
    calls: list[str] = []
    core.delete_namespace.side_effect = lambda **_: calls.append("namespace")
    core.delete_collection_namespaced_pod.side_effect = lambda *_, **__: calls.append("pods")

    kubectl.delete_namespace("hotel-reservation")

    # Controllers stop recreating pods once the namespace is terminating.
    assert calls == ["namespace", "pods"]
    core.delete_collection_namespaced_pod.assert_called_once_with("hotel-reservation", grace_period_seconds=0)


def test_fast_teardown_failure_falls_back_to_normal_deletion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SREGYM_FAST_NAMESPACE_TEARDOWN", "1")
    kubectl, core = _kubectl()
    core.delete_collection_namespaced_pod.side_effect = RuntimeError("forbidden")

    kubectl.delete_namespace("hotel-reservation")

    core.read_namespace.assert_called()
