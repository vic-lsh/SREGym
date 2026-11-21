"""Storage the platform provides: a lane whose StorageClasses come from its host (a vcluster lane) cannot hold the
OpenEBS classes SREGym otherwise installs, so the observability claims use the cluster's default class."""

from __future__ import annotations

import os

import yaml

MANAGED_STORAGE_ENV = "SREGYM_MANAGED_STORAGE"


def managed_storage() -> bool:
    return os.environ.get(MANAGED_STORAGE_ENV, "").strip().lower() in {"1", "true", "yes"}


def default_class_manifest(text: str) -> str:
    """A PersistentVolumeClaim manifest without ``storageClassName``, so the default class serves it."""
    claim = yaml.safe_load(text)
    claim.get("spec", {}).pop("storageClassName", None)
    return yaml.safe_dump(claim)
