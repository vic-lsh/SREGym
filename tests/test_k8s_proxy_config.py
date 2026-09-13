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
