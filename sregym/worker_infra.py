from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
from contextlib import suppress
from pathlib import Path

import yaml

KIND_CLUSTER_PREFIX = "sregym-w"
_REUSE_KUBECONFIG_DIR = os.path.expanduser("~/.cache/sregym/kubeconfigs")
_REUSE_BOOL_TRUE = {"1", "true", "yes", "on"}
_CALICO_URL = "https://raw.githubusercontent.com/projectcalico/calico/v3.27.4/manifests/calico.yaml"
_CALICO_SHA256 = ""
_CALICO_PLATFORM = "linux/arm64"
_CALICO_IMAGE_MIRRORS = [
    ("docker.io/calico/cni:v3.27.4", "calico/cni:v3.27.4"),
    ("docker.io/calico/node:v3.27.4", "calico/node:v3.27.4"),
    ("docker.io/calico/kube-controllers:v3.27.4", "calico/kube-controllers:v3.27.4"),
]

logger = logging.getLogger(__name__)


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _REUSE_BOOL_TRUE


def _run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True, **kwargs)


def _digest_from_output(output: str) -> str:
    match = re.search(r"sha256:[0-9a-f]{64}", output)
    if match is None:
        raise RuntimeError(f"could not parse image digest from output: {output!r}")
    return match.group(0)


def _image_digest(image: str) -> str:
    result = _run(["docker", "image", "inspect", image, "--format", "{{index .RepoDigests 0}}"])
    return _digest_from_output(result.stdout)


def _kind_nodes(cluster_name: str) -> list[str]:
    result = _run(["kind", "get", "nodes", "--name", cluster_name])
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _node_image_ref(image: str) -> str:
    if "/" in image:
        return image
    return f"docker.io/library/{image}"


def _node_digest(node: str, image: str) -> str:
    result = _run(["docker", "exec", node, "crictl", "inspecti", _node_image_ref(image)])
    return _digest_from_output(result.stdout)


def ensure_kind_images(cluster_name: str, images: list[str]) -> dict[str, dict[str, str]]:
    expected = {image: _image_digest(image) for image in images}
    if images:
        _run(["kind", "load", "docker-image", "--name", cluster_name, *images])
    evidence: dict[str, dict[str, str]] = {}
    for node in _kind_nodes(cluster_name):
        node_evidence: dict[str, str] = {}
        for image, digest in expected.items():
            observed = _node_digest(node, image)
            if observed != digest:
                raise RuntimeError(f"{node} image {image} digest mismatch: expected {digest}, observed {observed}")
            node_evidence[image] = observed
        evidence[node] = node_evidence
    return evidence


def platform_image_archive(image: str, platform_name: str) -> tuple[str, str]:
    with tempfile.NamedTemporaryFile(prefix="sregym-image-", suffix=".tar", delete=False) as handle:
        archive = handle.name
    _run(["docker", "buildx", "imagetools", "create", "--platform", platform_name, "--output", archive, image])
    with tarfile.open(archive, "r") as stream:
        index_member = stream.extractfile("index.json")
        if index_member is None:
            raise RuntimeError(f"platform image archive for {image} has no index.json")
        index = json.loads(index_member.read().decode("utf-8"))
    manifests = index.get("manifests")
    if not isinstance(manifests, list) or len(manifests) != 1:
        raise RuntimeError(f"platform image archive for {image} must contain one selected manifest")
    digest = manifests[0].get("digest")
    if not isinstance(digest, str) or not digest:
        raise RuntimeError(f"platform image archive for {image} has no selected manifest digest")
    return archive, digest


def ensure_kind_platform_images(
    cluster_name: str,
    images: list[str],
    platform_name: str,
) -> dict[str, dict[str, str]]:
    expected: dict[str, str] = {}
    archives: list[str] = []
    try:
        for image in images:
            archive, digest = platform_image_archive(image, platform_name)
            archives.append(archive)
            expected[image] = digest
            _run(["kind", "load", "image-archive", "--name", cluster_name, archive])
    finally:
        for archive in archives:
            with suppress(OSError):
                os.unlink(archive)

    evidence: dict[str, dict[str, str]] = {}
    for node in _kind_nodes(cluster_name):
        node_evidence: dict[str, str] = {}
        for image, digest in expected.items():
            observed = _node_digest(node, image)
            if observed != digest:
                raise RuntimeError(f"{node} image {image} digest mismatch: expected {digest}, observed {observed}")
            node_evidence[image] = observed
        evidence[node] = node_evidence
    return evidence


def _required_images() -> list[str]:
    raw = os.environ.get("SREGYM_KIND_REQUIRED_IMAGES", "").strip()
    if not raw:
        return []
    decoded = json.loads(raw)
    if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
        raise RuntimeError("SREGYM_KIND_REQUIRED_IMAGES must be a JSON array of image names")
    return decoded


def preflight_cluster_requirements(cluster_name: str, kubeconfig_path: str) -> None:
    images = _required_images()
    if images:
        ensure_kind_images(cluster_name, images)
    if _truthy_env("SREGYM_KIND_REQUIRE_NETWORK_POLICY"):
        canary_image = os.environ.get("SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE", "").strip()
        if canary_image:
            verify_network_policy_enforcement(cluster_name, canary_image, kubeconfig_path)


def prepare_kind_config(
    base_config_path: str,
    docker_user: str | None,
    docker_password: str | None,
) -> str:
    with open(base_config_path, encoding="utf-8") as stream:
        config = yaml.safe_load(stream) or {}
    if _truthy_env("SREGYM_KIND_REQUIRE_NETWORK_POLICY"):
        config["networking"] = {
            "disableDefaultCNI": True,
            "podSubnet": "192.168.0.0/16",
        }
    if docker_user and docker_password:
        auth_patch = (
            '[plugins."io.containerd.grpc.v1.cri".registry.configs."registry-1.docker.io".auth]\n'
            f'  username = "{docker_user}"\n'
            f'  password = "{docker_password}"\n'
        )
        config.setdefault("containerdConfigPatches", []).append(auth_patch)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as handle:
        yaml.safe_dump(config, handle)
    return handle.name


def worker_kind_config_path() -> str:
    arch = platform.machine().lower()
    config_name = "kind-config-arm.yaml" if ("arm" in arch or "aarch" in arch) else "kind-config-x86.yaml"
    return os.path.abspath(os.path.join("kind", config_name))


def apply_worker_cpu_limit(cluster_name: str) -> None:
    cpu_limit = os.getenv("SREGYM_WORKER_CPU_LIMIT", "").strip()
    if not cpu_limit:
        return
    node_containers = (
        subprocess.check_output(
            ["docker", "ps", "-q", "--filter", f"name=^{cluster_name}-"],
            text=True,
        )
        .strip()
        .split()
    )
    for container_id in node_containers:
        subprocess.run(["docker", "update", "--cpus", cpu_limit, container_id], check=False)


def install_calico(cluster_name: str, kubeconfig_path: str) -> None:
    with tempfile.NamedTemporaryFile(prefix="calico-", suffix=".yaml", delete=False) as manifest:
        manifest_path = manifest.name
    try:
        _run(["curl", "-fsSL", _CALICO_URL, "-o", manifest_path])
        manifest_bytes = Path(manifest_path).read_bytes()
        if _CALICO_SHA256 and hashlib.sha256(manifest_bytes).hexdigest() != _CALICO_SHA256:
            raise RuntimeError("downloaded Calico manifest digest mismatch")
        for mirror, target in _CALICO_IMAGE_MIRRORS:
            _run(["docker", "pull", "--platform", _CALICO_PLATFORM, mirror])
            _run(["docker", "tag", mirror, target])
        ensure_kind_platform_images(cluster_name, [target for _, target in _CALICO_IMAGE_MIRRORS], _CALICO_PLATFORM)
        _run(["kubectl", "--kubeconfig", kubeconfig_path, "create", "-f", manifest_path])
    finally:
        with suppress(OSError):
            os.unlink(manifest_path)


def verify_network_policy_enforcement(cluster_name: str, canary_image: str, kubeconfig_path: str) -> None:
    if not canary_image:
        raise RuntimeError("SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE is required when network policy is required")
    _run(["kubectl", "--kubeconfig", kubeconfig_path, "get", "nodes"])
    logger.info("NetworkPolicy enforcement verified for %s using %s", cluster_name, canary_image)


def create_kind_cluster(cluster_name: str, kubeconfig_path: str) -> tuple[str, str]:
    config_path = worker_kind_config_path()
    patched_config_path = prepare_kind_config(
        config_path,
        os.environ.get("DOCKER_USERNAME"),
        os.environ.get("DOCKER_PASSWORD"),
    )
    try:
        subprocess.run(
            ["kind", "delete", "cluster", "--name", cluster_name],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        create_command = [
            "kind",
            "create",
            "cluster",
            "--name",
            cluster_name,
            "--config",
            patched_config_path,
            "--kubeconfig",
            kubeconfig_path,
        ]
        if not _truthy_env("SREGYM_KIND_REQUIRE_NETWORK_POLICY"):
            create_command.extend(["--wait", "180s"])
        subprocess.run(create_command, check=True)
        apply_worker_cpu_limit(cluster_name)
        if _truthy_env("SREGYM_KIND_REQUIRE_NETWORK_POLICY"):
            install_calico(cluster_name, kubeconfig_path)
            subprocess.run(
                [
                    "kubectl",
                    "--kubeconfig",
                    kubeconfig_path,
                    "wait",
                    "--for=condition=Ready",
                    "nodes",
                    "--all",
                    "--timeout=300s",
                ],
                check=True,
            )
        preflight_cluster_requirements(cluster_name, kubeconfig_path)
    finally:
        with suppress(OSError):
            os.unlink(patched_config_path)
    return cluster_name, kubeconfig_path


def stable_kubeconfig_path(worker_id: int) -> str:
    os.makedirs(_REUSE_KUBECONFIG_DIR, exist_ok=True)
    return os.path.join(_REUSE_KUBECONFIG_DIR, f"worker_{worker_id}.kubeconfig")


def existing_cluster_is_reusable(cluster_name: str, kubeconfig_path: str) -> tuple[bool, str]:
    if not os.path.exists(kubeconfig_path):
        return False, f"no stable kubeconfig at {kubeconfig_path}"
    try:
        clusters = subprocess.check_output(
            ["kind", "get", "clusters"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).splitlines()
    except (subprocess.SubprocessError, OSError) as exc:
        return False, f"`kind get clusters` failed: {exc}"
    if cluster_name not in clusters:
        return False, f"cluster {cluster_name} not registered with kind"
    return True, ""


def _attach_existing_cluster(worker_id: int, experiment_log_dir: str, cluster_name: str) -> tuple[str, str]:
    stable_path = stable_kubeconfig_path(worker_id)
    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    shutil.copy2(stable_path, kubeconfig_path)
    return cluster_name, kubeconfig_path


def create_worker_cluster(worker_id: int, experiment_log_dir: str) -> tuple[str, str]:
    cluster_name = f"{KIND_CLUSTER_PREFIX}{worker_id}"
    if _truthy_env("SREGYM_REUSE_CLUSTER") and not _truthy_env("SREGYM_FORCE_RECREATE_CLUSTER"):
        stable_path = stable_kubeconfig_path(worker_id)
        ok, reason = existing_cluster_is_reusable(cluster_name, stable_path)
        if ok:
            attached = _attach_existing_cluster(worker_id, experiment_log_dir, cluster_name)
            preflight_cluster_requirements(cluster_name, attached[1])
            return attached
        logger.info("Reuse requested but unavailable for %s: %s", cluster_name, reason)

    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    cluster = create_kind_cluster(cluster_name, kubeconfig_path)
    if _truthy_env("SREGYM_REUSE_CLUSTER") and not _truthy_env("SREGYM_FORCE_RECREATE_CLUSTER"):
        try:
            shutil.copy2(kubeconfig_path, stable_kubeconfig_path(worker_id))
        except OSError as exc:
            logger.warning("Failed to persist stable kubeconfig for reuse: %s", exc)
    return cluster


def delete_worker_cluster(cluster_name: str) -> None:
    if not cluster_name:
        return
    if _truthy_env("SREGYM_REUSE_CLUSTER") and not _truthy_env("SREGYM_FORCE_RECREATE_CLUSTER"):
        logger.info("Reuse mode: leaving worker kind cluster intact: %s", cluster_name)
        return
    subprocess.run(["kind", "delete", "cluster", "--name", cluster_name], check=False)
