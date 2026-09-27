from sregym.service import k8s_proxy


def test_proxy_prefers_supervisor_base_kubeconfig(monkeypatch) -> None:
    loaded = []
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", "/tmp/worker.kubeconfig")
    monkeypatch.setenv("KUBECONFIG", "/tmp/proxy.kubeconfig")
    monkeypatch.setattr(k8s_proxy.os.path, "exists", lambda path: False)
    monkeypatch.setattr(k8s_proxy.config, "load_kube_config", lambda config_file: loaded.append(config_file))
    monkeypatch.setattr(
        k8s_proxy.KubernetesAPIProxy,
        "_load_cluster_config",
        lambda self, kubeconfig_path: ("127.0.0.1", 6443, "ca", "cert", "key"),
    )

    k8s_proxy.KubernetesAPIProxy()

    assert loaded == ["/tmp/worker.kubeconfig"]


def test_agent_proxy_port_is_distinct_per_worker(monkeypatch) -> None:
    monkeypatch.delenv("SREGYM_WORKER_ID", raising=False)
    assert k8s_proxy.agent_proxy_port() == 16443
    monkeypatch.setenv("SREGYM_WORKER_ID", "2")
    assert k8s_proxy.agent_proxy_port() == 16445


def test_cluster_baseline_state_file_is_keyed_by_cluster(monkeypatch) -> None:
    from sregym import paths

    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    assert paths.cluster_baseline_state_file() == paths.CLUSTER_BASELINE_STATE_FILE
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-w1")
    assert paths.cluster_baseline_state_file() == paths.CACHE_DIR / "cluster_baseline_state.luna-w1.json"


def test_agent_kubeconfigs_of_concurrent_workers_do_not_share_a_file(monkeypatch, tmp_path) -> None:
    """Every conductor used to write /tmp/sregym-agent-kubeconfig, so the last writer's proxy won."""

    monkeypatch.setattr(k8s_proxy.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(k8s_proxy.config, "load_kube_config", lambda config_file: None)
    monkeypatch.setattr(
        k8s_proxy.KubernetesAPIProxy,
        "_load_cluster_config",
        lambda self, kubeconfig_path: ("127.0.0.1", 6443, "ca", "cert", "key"),
    )
    monkeypatch.setattr(k8s_proxy.os.path, "exists", lambda path: False)

    first = k8s_proxy.KubernetesAPIProxy(listen_port=16443).generate_agent_kubeconfig()
    second = k8s_proxy.KubernetesAPIProxy(listen_port=16444).generate_agent_kubeconfig()

    assert first != second
    assert "16443" in open(first).read()
    assert "16444" in open(second).read()
