"""A lane whose storage is the platform's (a vcluster lane: StorageClasses come from the host) installs no OpenEBS and
lets the observability PVCs use the cluster's default class."""

from __future__ import annotations

import yaml

from sregym.service import storage
from sregym.service.telemetry.prometheus import Prometheus


def test_managed_storage_is_off_unless_asked(monkeypatch) -> None:
    monkeypatch.delenv(storage.MANAGED_STORAGE_ENV, raising=False)
    assert not storage.managed_storage()
    monkeypatch.setenv(storage.MANAGED_STORAGE_ENV, "1")
    assert storage.managed_storage()


def test_the_claim_drops_its_storage_class_so_the_default_class_serves_it() -> None:
    text = "kind: PersistentVolumeClaim\nspec:\n  storageClassName: openebs-hostpath\n  accessModes: [ReadWriteOnce]\n"
    assert "storageClassName" not in yaml.safe_load(storage.default_class_manifest(text))["spec"]


def test_prometheus_helm_uses_the_default_class_on_managed_storage(monkeypatch) -> None:
    monkeypatch.setenv(storage.MANAGED_STORAGE_ENV, "1")
    configs = Prometheus().helm_configs
    assert "--set server.persistentVolume.storageClass=null" in configs["extra_args"]


def test_prometheus_helm_keeps_its_class_otherwise(monkeypatch) -> None:
    monkeypatch.delenv(storage.MANAGED_STORAGE_ENV, raising=False)
    assert not Prometheus().helm_configs.get("extra_args")
