"""Concurrent experiments on separate kind clusters must never share state; contamination fails loudly."""

from __future__ import annotations

import subprocess

import pytest
import yaml

from sregym import paths
from sregym.service import k8s_proxy
from sregym.worker_infra import ClusterInUseError, cluster_lock


def _agent_kubeconfig(path, port: int) -> str:
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "sregym-agent",
                "clusters": [{"name": "sregym-proxy", "cluster": {"server": f"http://127.0.0.1:{port}"}}],
                "contexts": [{"name": "sregym-agent", "context": {"cluster": "sregym-proxy", "user": "a"}}],
                "users": [{"name": "a", "user": {}}],
            }
        )
    )
    return str(path)


def _nodes(*names: str):
    def run(command, **kwargs):
        assert command[:2] == ["kubectl", "--kubeconfig"]
        return subprocess.CompletedProcess(command, 0, "".join(f"node/{name}\n" for name in names), "")

    return run


def test_agent_kubeconfig_guard_accepts_its_own_proxy_and_cluster(tmp_path) -> None:
    path = _agent_kubeconfig(tmp_path / "k", 16444)

    k8s_proxy.verify_agent_kubeconfig(
        path, listen_port=16444, cluster_name="luna-w1", runner=_nodes("luna-w1-control-plane", "luna-w1-worker")
    )


def test_agent_kubeconfig_guard_rejects_another_workers_proxy(tmp_path) -> None:
    path = _agent_kubeconfig(tmp_path / "k", 16445)

    with pytest.raises(k8s_proxy.AgentKubeconfigMismatch, match="16445.*16444"):
        k8s_proxy.verify_agent_kubeconfig(path, listen_port=16444, cluster_name="luna-w1", runner=_nodes("luna-w1-x"))


def test_agent_kubeconfig_guard_rejects_nodes_of_another_cluster(tmp_path) -> None:
    path = _agent_kubeconfig(tmp_path / "k", 16443)

    with pytest.raises(k8s_proxy.AgentKubeconfigMismatch, match="luna-w2-worker"):
        k8s_proxy.verify_agent_kubeconfig(
            path, listen_port=16443, cluster_name="luna-w0", runner=_nodes("luna-w2-control-plane", "luna-w2-worker")
        )


def test_second_experiment_on_the_same_cluster_fails_loudly(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("sregym.worker_infra._LOCK_DIR", str(tmp_path))

    with cluster_lock("luna-w1"):
        with pytest.raises(ClusterInUseError, match="luna-w1"), cluster_lock("luna-w1"):
            pass
        with cluster_lock("luna-w2"):
            pass
    with cluster_lock("luna-w1"):
        pass


def test_fault_scratch_paths_are_per_cluster(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(paths, "FAULT_SCRATCH_ROOT", tmp_path)
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    assert paths.fault_scratch_path("geo_modified.yaml") == str(tmp_path / "geo_modified.yaml")

    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-w1")
    first = paths.fault_scratch_path("geo_modified.yaml")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-w2")
    second = paths.fault_scratch_path("geo_modified.yaml")

    assert first != second
    assert first == str(tmp_path / "sregym-luna-w1" / "geo_modified.yaml")
    assert (tmp_path / "sregym-luna-w2").is_dir()


def test_fault_injectors_keep_no_fixed_tmp_paths() -> None:
    from pathlib import Path

    source = (Path(paths.BASE_DIR) / "generators" / "fault" / "inject_virtual.py").read_text()
    assert 'f"/tmp/' not in source
    # A recovery that reads a fixed /tmp backup never finds the per-cluster one its injection wrote.
    assert "/tmp/{" not in source
