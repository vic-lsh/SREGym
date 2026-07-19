"""Unit tests for fork-specific worker-cluster preparation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from sregym import worker_infra


def test_prepare_kind_config_adds_registry_auth_and_network_policy(tmp_path, monkeypatch):
    base = tmp_path / "kind.yml"
    base.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n", encoding="utf-8")
    monkeypatch.setenv("SREGYM_KIND_REQUIRE_NETWORK_POLICY", "true")

    generated = Path(worker_infra.prepare_kind_config(str(base), "user", "password"))
    try:
        config = yaml.safe_load(generated.read_text(encoding="utf-8"))
        assert config["networking"]["disableDefaultCNI"] is True
        assert 'username = "user"' in config["containerdConfigPatches"][0]
    finally:
        generated.unlink()


def test_required_images_validates_json_shape(monkeypatch):
    monkeypatch.setenv("SREGYM_KIND_REQUIRED_IMAGES", json.dumps({"image": "not-a-list"}))
    with pytest.raises(RuntimeError, match="JSON array"):
        worker_infra._required_images()


def test_existing_cluster_reuse_requires_stable_kubeconfig(tmp_path):
    missing = tmp_path / "missing-kubeconfig"
    reusable, reason = worker_infra.existing_cluster_is_reusable("sregym-w0", str(missing))
    assert reusable is False
    assert "no stable kubeconfig" in reason


def test_delete_worker_cluster_preserves_reused_cluster(monkeypatch):
    monkeypatch.setenv("SREGYM_REUSE_CLUSTER", "1")
    monkeypatch.setattr(worker_infra.subprocess, "run", lambda *args, **kwargs: pytest.fail("unexpected delete"))
    worker_infra.delete_worker_cluster("sregym-w0")


def test_network_policy_preflight_uses_worker_kubeconfig(monkeypatch):
    commands = []
    monkeypatch.setattr(worker_infra, "_run", lambda command: commands.append(command))

    worker_infra.verify_network_policy_enforcement(
        "sregym-w0",
        "network-policy-canary:latest",
        "/tmp/worker-0.kubeconfig",
    )

    assert commands == [["kubectl", "--kubeconfig", "/tmp/worker-0.kubeconfig", "get", "nodes"]]
