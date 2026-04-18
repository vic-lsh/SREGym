"""Worker-cluster infrastructure shared between the experiment runner and
stress-test driver.

These helpers used to live directly in ``bench/sregym/main.py``; they were
extracted so ``bench/sregym/stress_test.py`` can reuse them without pulling
in main.py's MCP-server and AgentLauncher side effects.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from typing import Optional

logger = logging.getLogger("all.worker_infra")

KIND_CLUSTER_PREFIX = "sregym-w"

_REUSE_KUBECONFIG_DIR = os.path.expanduser("~/.cache/sregym/kubeconfigs")
_REUSE_BOOL_TRUE = {"1", "true", "yes", "on"}


def worker_kind_config_path() -> str:
    """Choose the kind config file based on host architecture."""
    import platform
    arch = platform.machine().lower()
    config_name = "kind-config-arm.yaml" if ("arm" in arch or "aarch" in arch) else "kind-config-x86.yaml"
    return os.path.abspath(os.path.join("kind", config_name))


def apply_worker_cpu_limit(cluster_name: str) -> None:
    """Apply per-node CPU cap to kind cluster containers if SREGYM_WORKER_CPU_LIMIT is set."""
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
    for cid in node_containers:
        subprocess.run(["docker", "update", "--cpus", cpu_limit, cid], check=False)
    logger.info(f"Applied CPU limit of {cpu_limit} to {len(node_containers)} node containers for {cluster_name}")


def log_cpu_oversubscription(num_workers: int) -> None:
    """Log CPU oversubscription ratio when SREGYM_WORKER_CPU_LIMIT is set."""
    cpu_limit = os.getenv("SREGYM_WORKER_CPU_LIMIT", "").strip()
    if not cpu_limit:
        return
    try:
        limit_per_node = float(cpu_limit)
    except ValueError:
        return
    config_path = worker_kind_config_path()
    try:
        with open(config_path) as f:
            nodes_per_cluster = sum(1 for line in f if line.strip().startswith("- role:"))
    except OSError:
        return
    host_cpus = os.cpu_count() or 1
    total_allocated = limit_per_node * nodes_per_cluster * num_workers
    ratio = total_allocated / host_cpus
    logger.info(
        f"CPU oversubscription: {limit_per_node} cpus/node × {nodes_per_cluster} nodes/cluster "
        f"× {num_workers} workers = {total_allocated:.0f} allocated vs {host_cpus} host CPUs "
        f"(ratio: {ratio:.2f}x)"
    )
    if ratio > 1.0:
        logger.warning(f"CPU is oversubscribed by {ratio:.2f}x — expect contention under load.")


def _build_kind_config_with_registry_auth(base_config_path: str, docker_user: str, docker_password: str) -> str:
    """Return path to a temp kind config with Docker Hub auth injected."""
    import yaml

    with open(base_config_path) as f:
        config = yaml.safe_load(f)

    auth_patch = (
        '[plugins."io.containerd.grpc.v1.cri".registry.configs."registry-1.docker.io".auth]\n'
        f'  username = "{docker_user}"\n'
        f'  password = "{docker_password}"\n'
    )
    config.setdefault("containerdConfigPatches", [])
    config["containerdConfigPatches"].append(auth_patch)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.dump(config, tmp)
    tmp.flush()
    return tmp.name


def reuse_cluster_enabled() -> bool:
    """True iff SREGYM_REUSE_CLUSTER is set and SREGYM_FORCE_RECREATE_CLUSTER is not."""
    reuse = os.environ.get("SREGYM_REUSE_CLUSTER", "").strip().lower() in _REUSE_BOOL_TRUE
    force = os.environ.get("SREGYM_FORCE_RECREATE_CLUSTER", "").strip().lower() in _REUSE_BOOL_TRUE
    return reuse and not force


def stable_kubeconfig_path(worker_id: int) -> str:
    """Per-worker kubeconfig path that survives across `run_sregym.sh` invocations."""
    os.makedirs(_REUSE_KUBECONFIG_DIR, exist_ok=True)
    return os.path.join(_REUSE_KUBECONFIG_DIR, f"worker_{worker_id}.kubeconfig")


def existing_cluster_is_reusable(cluster_name: str, kubeconfig_path: str) -> tuple[bool, str]:
    """Probe whether `cluster_name` exists and is healthy enough to reuse."""
    if not os.path.exists(kubeconfig_path):
        return False, f"no stable kubeconfig at {kubeconfig_path}"
    try:
        clusters = subprocess.check_output(
            ["kind", "get", "clusters"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).splitlines()
    except (subprocess.SubprocessError, OSError) as e:
        return False, f"`kind get clusters` failed: {e}"
    if cluster_name not in clusters:
        return False, f"cluster {cluster_name} not registered with kind"
    try:
        result = subprocess.run(
            ["kubectl", "--kubeconfig", kubeconfig_path, "get", "nodes", "--no-headers"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.SubprocessError, OSError) as e:
        return False, f"kubectl get nodes failed: {e}"
    node_lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    if not node_lines:
        return False, "no nodes found in existing cluster"
    not_ready = [ln.split()[0] for ln in node_lines if " Ready" not in ln]
    if not_ready:
        return False, f"nodes not Ready: {not_ready}"
    return True, ""


def _attach_existing_cluster(
    worker_id: int, experiment_log_dir: str, cluster_name: str
) -> tuple[str, str]:
    """Wire up env vars and per-run kubeconfig copy for a reused cluster."""
    stable_path = stable_kubeconfig_path(worker_id)
    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    shutil.copy2(stable_path, kubeconfig_path)

    os.environ["KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_KIND_CLUSTER_NAME"] = cluster_name

    logger.info(
        f"Worker {worker_id} reusing existing cluster {cluster_name}; "
        f"kubeconfig={kubeconfig_path}"
    )
    return cluster_name, kubeconfig_path


def create_kind_cluster(cluster_name: str, kubeconfig_path: str) -> None:
    config_path = worker_kind_config_path()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Kind config file not found: {config_path}")

    docker_user = os.environ.get("DOCKER_USERNAME")
    docker_password = os.environ.get("DOCKER_PASSWORD")
    patched_config_path: Optional[str] = None
    if docker_user and docker_password:
        patched_config_path = _build_kind_config_with_registry_auth(config_path, docker_user, docker_password)
        config_path = patched_config_path
        logger.info("Docker Hub credentials will be injected into containerd on all kind nodes.")
    else:
        logger.warning(
            "DOCKER_USERNAME/DOCKER_PASSWORD not set. Kind nodes will pull Docker Hub images unauthenticated."
        )

    logger.info(f"Preparing isolated kind cluster: {cluster_name}")

    try:
        cmd = ["docker", "ps", "-a", "-q", "--filter", f"name=^{cluster_name}-"]
        container_ids = subprocess.check_output(cmd, text=True).strip().split()
        if container_ids:
            logger.info(f"Force removing lingering containers for {cluster_name}: {container_ids}")
            subprocess.run(
                ["docker", "rm", "-f"] + container_ids,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception as exc:
        logger.warning(f"Failed to force cleanup containers for {cluster_name}: {exc}")

    subprocess.run(
        ["kind", "delete", "cluster", "--name", cluster_name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    try:
        subprocess.run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                cluster_name,
                "--config",
                config_path,
                "--kubeconfig",
                kubeconfig_path,
                "--wait",
                "180s",
            ],
            check=True,
        )
    finally:
        if patched_config_path and os.path.exists(patched_config_path):
            os.unlink(patched_config_path)

    apply_worker_cpu_limit(cluster_name)

    os.environ["KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_KIND_CLUSTER_NAME"] = cluster_name

    subprocess.run(
        ["kubectl", "config", "current-context", "--kubeconfig", kubeconfig_path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def create_worker_cluster(worker_id: int, experiment_log_dir: str) -> tuple[str, str]:
    """Create a dedicated kind cluster for one worker and return (cluster_name, kubeconfig_path).

    When SREGYM_REUSE_CLUSTER is set (and SREGYM_FORCE_RECREATE_CLUSTER is not),
    an existing healthy `sregym-w{id}` cluster is reused instead of being torn
    down and recreated. If reuse is requested but the cluster is missing or
    unhealthy, falls back to a full recreate.
    """
    cluster_name = f"{KIND_CLUSTER_PREFIX}{worker_id}"

    if reuse_cluster_enabled():
        stable_path = stable_kubeconfig_path(worker_id)
        ok, reason = existing_cluster_is_reusable(cluster_name, stable_path)
        if ok:
            return _attach_existing_cluster(worker_id, experiment_log_dir, cluster_name)
        logger.info(
            f"Reuse requested but unavailable for {cluster_name}: {reason}. "
            f"Falling back to full recreate."
        )

    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    create_kind_cluster(cluster_name, kubeconfig_path)

    if reuse_cluster_enabled():
        try:
            shutil.copy2(kubeconfig_path, stable_kubeconfig_path(worker_id))
        except OSError as e:
            logger.warning(f"Failed to persist stable kubeconfig for reuse: {e}")

    logger.info(f"Worker {worker_id} cluster ready: {cluster_name}, kubeconfig={kubeconfig_path}")
    return cluster_name, kubeconfig_path


def delete_kind_cluster(cluster_name: str) -> None:
    if not cluster_name:
        return
    logger.info(f"Tearing down kind cluster: {cluster_name}")
    subprocess.run(
        ["kind", "delete", "cluster", "--name", cluster_name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def delete_worker_cluster(cluster_name: str) -> None:
    """Delete a worker's dedicated kind cluster (honours SREGYM_REUSE_CLUSTER)."""
    if not cluster_name:
        return
    if reuse_cluster_enabled():
        logger.info(f"Reuse mode: leaving worker kind cluster intact: {cluster_name}")
        return
    delete_kind_cluster(cluster_name)
