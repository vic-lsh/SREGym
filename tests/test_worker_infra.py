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


def test_node_digests_include_imported_repo_digest(monkeypatch):
    output = '{"status":{"id":"sha256:' + "1" * 64 + '","repoDigests":["image@sha256:' + "2" * 64 + '"]}}'
    monkeypatch.setattr(
        worker_infra,
        "_run",
        lambda command: type("Result", (), {"stdout": output})(),
    )

    assert worker_infra._node_digests("node", "image:tag") == {
        "sha256:" + "1" * 64,
        "sha256:" + "2" * 64,
    }


def test_existing_cluster_reuse_requires_stable_kubeconfig(tmp_path):
    missing = tmp_path / "missing-kubeconfig"
    reusable, reason = worker_infra.existing_cluster_is_reusable("sregym-w0", str(missing))
    assert reusable is False
    assert "no stable kubeconfig" in reason


def test_stable_kubeconfig_is_namespaced_by_cluster_prefix(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_infra, "_REUSE_KUBECONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(worker_infra, "KIND_CLUSTER_PREFIX", "reuse-a-w")
    first = worker_infra.stable_kubeconfig_path(0)
    monkeypatch.setattr(worker_infra, "KIND_CLUSTER_PREFIX", "reuse-b-w")
    second = worker_infra.stable_kubeconfig_path(0)

    assert first != second
    assert first.endswith("reuse-a-w0.kubeconfig")
    assert second.endswith("reuse-b-w0.kubeconfig")


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


@pytest.mark.parametrize(
    ("machine", "expected"),
    [("x86_64", "linux/amd64"), ("aarch64", "linux/arm64")],
)
def test_container_platform_tracks_host_architecture(machine, expected, monkeypatch):
    monkeypatch.setattr(worker_infra.platform, "machine", lambda: machine)
    assert worker_infra.container_platform() == expected


def test_install_calico_loads_locally_selected_images(tmp_path, monkeypatch):
    commands = []
    loaded = []

    def fake_run(command):
        commands.append(command)
        if command[:2] == ["curl", "-fsSL"]:
            Path(command[-1]).write_text("kind: ConfigMap\n", encoding="utf-8")

    monkeypatch.setattr(worker_infra, "_CALICO_URL", "https://example.test/calico.yaml")
    monkeypatch.setattr(worker_infra, "_run", fake_run)
    monkeypatch.setattr(
        worker_infra,
        "ensure_kind_platform_images",
        lambda cluster, images, platform: loaded.append((cluster, images, platform)),
    )
    monkeypatch.setattr(worker_infra, "container_platform", lambda: "linux/amd64")

    worker_infra.install_calico("cluster", str(tmp_path / "kubeconfig"))

    assert loaded == [
        ("cluster", [target for _, target in worker_infra._CALICO_IMAGE_MIRRORS], "linux/amd64")
    ]
    assert all("--platform" in command for command in commands if command[:2] == ["docker", "pull"])


_THREE_WORKERS = """
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: kind-node:x86
  - role: worker
    image: kind-node:x86
    extraMounts: [{hostPath: /run/udev, containerPath: /run/udev}]
  - role: worker
    image: kind-node:x86
  - role: worker
    image: kind-node:x86
"""


def _prepared_nodes(tmp_path, monkeypatch, workers: str | None) -> list[dict]:
    base = tmp_path / "kind.yml"
    base.write_text(_THREE_WORKERS, encoding="utf-8")
    if workers is None:
        monkeypatch.delenv("SREGYM_KIND_WORKER_NODES", raising=False)
    else:
        monkeypatch.setenv("SREGYM_KIND_WORKER_NODES", workers)
    generated = Path(worker_infra.prepare_kind_config(str(base), None, None))
    try:
        return yaml.safe_load(generated.read_text(encoding="utf-8"))["nodes"]
    finally:
        generated.unlink()


def test_kind_topology_is_unchanged_without_a_worker_count(tmp_path, monkeypatch):
    nodes = _prepared_nodes(tmp_path, monkeypatch, None)

    assert [node["role"] for node in nodes] == ["control-plane", "worker", "worker", "worker"]


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_kind_worker_count_sizes_the_cluster_from_the_first_worker(tmp_path, monkeypatch, workers):
    nodes = _prepared_nodes(tmp_path, monkeypatch, str(workers))

    assert [node["role"] for node in nodes] == ["control-plane"] + ["worker"] * workers
    # Every worker keeps the base worker's image and mounts.
    assert all(node["image"] == "kind-node:x86" for node in nodes)
    assert all(node["extraMounts"] == [{"hostPath": "/run/udev", "containerPath": "/run/udev"}] for node in nodes[1:])


@pytest.mark.parametrize("workers", ["0", "-1", "two"])
def test_kind_worker_count_must_be_a_positive_integer(tmp_path, monkeypatch, workers):
    with pytest.raises(ValueError, match="SREGYM_KIND_WORKER_NODES"):
        _prepared_nodes(tmp_path, monkeypatch, workers)


def _reuse_candidate(tmp_path, monkeypatch, nodes: list[str]) -> tuple[bool, str]:
    kubeconfig = tmp_path / "stable.kubeconfig"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")

    def check_output(command, **kwargs):
        return "ready-w9\n" if command[:3] == ["kind", "get", "clusters"] else "\n".join(nodes) + "\n"

    monkeypatch.setattr(worker_infra.subprocess, "check_output", check_output)
    return worker_infra.existing_cluster_is_reusable("ready-w9", str(kubeconfig))


def test_a_reused_cluster_must_have_the_requested_worker_count(tmp_path, monkeypatch):
    three = ["ready-w9-control-plane", "ready-w9-worker", "ready-w9-worker2", "ready-w9-worker3"]
    monkeypatch.setenv("SREGYM_KIND_WORKER_NODES", "1")

    reusable, reason = _reuse_candidate(tmp_path, monkeypatch, three)
    assert reusable is False
    assert "3 worker nodes, not the requested 1" in reason

    assert _reuse_candidate(tmp_path, monkeypatch, three[:2]) == (True, "")
    monkeypatch.delenv("SREGYM_KIND_WORKER_NODES")
    assert _reuse_candidate(tmp_path, monkeypatch, three) == (True, "")
