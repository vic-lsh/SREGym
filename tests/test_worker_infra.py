"""Unit tests for fork-specific worker-cluster preparation."""

from __future__ import annotations

import io
import json
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml

from sregym import worker_infra


@pytest.mark.parametrize("node_identity", ["config", "target"])
@pytest.mark.parametrize("cached", [True, False])
def test_image_preflight_accepts_config_or_target_and_only_loads_stale_images(monkeypatch, node_identity, cached):
    config = "sha256:" + "a" * 64
    target = "sha256:" + "b" * 64
    stale = "sha256:" + "c" * 64
    expected = config if node_identity == "config" else target
    nodes = {"node-a": expected, "node-b": expected if cached else stale}
    loads = []

    def run(command):
        if command[:3] == ["docker", "image", "inspect"]:
            output = config if command[-1] == "{{.Id}}" else f"{config} image:tag@{target}"
        elif command[:3] == ["kind", "get", "nodes"]:
            output = "\n".join(nodes)
        elif command[:3] == ["kind", "load", "docker-image"]:
            loads.append(command)
            nodes["node-b"] = expected
            output = ""
        elif "crictl" in command:
            output = nodes[command[2]] if node_identity == "config" else stale
        elif "ctr" in command:
            output = f"image:tag @{nodes[command[2]]}" if node_identity == "target" else ""
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(worker_infra, "_run", run)

    assert worker_infra.ensure_kind_images("cluster", ["image:tag"]) == {
        node: {"image:tag": expected} for node in nodes
    }
    assert loads == ([] if cached else [["kind", "load", "docker-image", "--name", "cluster", "image:tag"]])


def test_image_preflight_rejects_stale_digest_after_load(monkeypatch):
    expected = "sha256:" + "a" * 64
    stale = "sha256:" + "b" * 64

    def run(command):
        if command[:3] == ["docker", "image", "inspect"]:
            output = expected
        elif command[:3] == ["kind", "get", "nodes"]:
            output = "node-a"
        else:
            output = stale
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(worker_infra, "_run", run)
    with pytest.raises(RuntimeError, match="node-a image image:tag digest mismatch"):
        worker_infra.ensure_kind_images("cluster", ["image:tag"])


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


def test_prepare_kind_config_enables_containerd_certs_d_for_registry_hosts(tmp_path):
    """SDO's in-cluster registry is wired through /etc/containerd/certs.d, which node images may not load."""
    base = tmp_path / "kind.yml"
    base.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n", encoding="utf-8")

    generated = Path(worker_infra.prepare_kind_config(str(base), None, None))
    try:
        config = yaml.safe_load(generated.read_text(encoding="utf-8"))
        patches = "\n".join(config["containerdConfigPatches"])
        for plugin in ("io.containerd.grpc.v1.cri", "io.containerd.cri.v1.images"):
            assert f'[plugins."{plugin}".registry]\n  config_path = "/etc/containerd/certs.d"' in patches
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


def test_local_config_digest_uses_docker_image_id(monkeypatch):
    commands = []
    digest = "sha256:" + "3" * 64

    def fake_run(command):
        commands.append(command)
        return type("Result", (), {"stdout": digest})()

    monkeypatch.setattr(worker_infra, "_run", fake_run)

    assert worker_infra._local_config_digest("image:tag") == digest
    assert commands == [["docker", "image", "inspect", "image:tag", "--format", "{{.Id}}"]]


def test_archive_config_digest_accepts_legacy_json_config_name(tmp_path):
    digest = "4" * 64
    payload = json.dumps([{"Config": f"{digest}.json"}]).encode()
    archive = tmp_path / "image.tar"
    with tarfile.open(archive, "w") as saved:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        saved.addfile(info, io.BytesIO(payload))

    assert worker_infra._archive_config_digest(str(archive), "image:tag") == f"sha256:{digest}"


def test_node_digests_include_containerd_index_digest_when_cri_reports_no_repo_digests(monkeypatch):
    """containerd 2.x CRI leaves repoDigests empty for an imported manifest list; ctr still lists its digest."""
    cri = '{"status":{"id":"sha256:' + "1" * 64 + '","repoDigests":[]}}'
    ctr = (
        "docker.io/library/image:tag application/vnd.docker.distribution.manifest.list.v2+json sha256:"
        + "3" * 64
        + " 5.0 MiB linux/amd64"
    )

    commands = []

    def run(command):
        commands.append(command)
        return type("Result", (), {"stdout": ctr if "ctr" in command else cri})()

    monkeypatch.setattr(worker_infra, "_run", run)

    assert "sha256:" + "3" * 64 in worker_infra._node_digests("node", "image:tag")
    assert worker_infra._containerd_image_name("calico/cni:v3.27.4") == "docker.io/calico/cni:v3.27.4"
    assert worker_infra._containerd_image_name("registry.k8s.io/pause:3") == "registry.k8s.io/pause:3"
    assert any("name==docker.io/library/image:tag" in command for command in commands)


def test_existing_cluster_reuse_requires_stable_kubeconfig(tmp_path):
    missing = tmp_path / "missing-kubeconfig"
    reusable, reason = worker_infra.existing_cluster_is_reusable("sregym-w0", str(missing))
    assert reusable is False
    assert "no stable kubeconfig" in reason


def test_stable_kubeconfig_is_namespaced_by_cluster_prefix(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_infra, "_REUSE_KUBECONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_PREFIX", "reuse-a-w")
    first = worker_infra.stable_kubeconfig_path(0)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_PREFIX", "reuse-b-w")
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

    assert loaded == [("cluster", [target for _, target in worker_infra._CALICO_IMAGE_MIRRORS], "linux/amd64")]
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


@pytest.mark.parametrize("failed_probe", ["clusters", "nodes"])
def test_inventory_timeout_never_recreates_a_reused_cluster(tmp_path, monkeypatch, failed_probe):
    kubeconfig = tmp_path / "stable.kubeconfig"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    monkeypatch.setenv("SREGYM_REUSE_CLUSTER", "true")
    monkeypatch.delenv("SREGYM_FORCE_RECREATE_CLUSTER", raising=False)
    monkeypatch.setenv("SREGYM_KIND_WORKER_NODES", "1")
    monkeypatch.setattr(worker_infra, "stable_kubeconfig_path", lambda _: str(kubeconfig))

    def check_output(command, **kwargs):
        if command[2] == failed_probe:
            raise worker_infra.subprocess.TimeoutExpired(command, 10)
        return "sregym-w0\n"

    def create(*args):
        raise AssertionError("inventory failure must not destroy the existing cluster")

    monkeypatch.setattr(worker_infra.subprocess, "check_output", check_output)
    monkeypatch.setattr(worker_infra, "create_kind_cluster", create)
    with pytest.raises(RuntimeError, match="inventory"):
        worker_infra.create_worker_cluster(0, str(tmp_path))
