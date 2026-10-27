"""Accepted rendered source reaches the real deployment consumer unchanged."""


def test_astronomy_deploy_consumes_accepted_manifest_plan(monkeypatch, tmp_path):
    """The real source plan reaches deploy unchanged, without a second chart render."""
    import importlib
    from unittest.mock import Mock

    source = importlib.import_module("sregym.service.source_deploy")
    astronomy = importlib.import_module("sregym.service.apps.astronomy_shop")
    manifest = tmp_path / "accepted source" / "kubernetes/astronomy-shop.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: synthetic\ndata:\n  relay: accepted bytes\n"
    )
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(manifest.parents[1]))
    monkeypatch.setenv("SREGYM_DEPLOY_FROM_SOURCE", "1")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "synthetic-lane")
    monkeypatch.setattr(source._AstronomyShopAdapter, "ensure_images_loaded", lambda *a, **kw: None)
    install = Mock()
    monkeypatch.setattr(astronomy.Helm, "install", install)
    monkeypatch.setattr(astronomy.Helm, "assert_if_deployed", Mock())
    app = astronomy.AstronomyShop.__new__(astronomy.AstronomyShop)
    app.namespace = "astronomy-shop"
    app.name = "OpenTelemetry Demo Astronomy Shop"
    app.helm_configs = {"namespace": app.namespace}
    app.kubectl = Mock()
    app.kubectl.get_node_architectures.return_value = {"amd64"}
    app.deploy()
    import shlex

    argv = shlex.split(app.kubectl.exec_command.call_args.args[0])
    assert argv == ["kubectl", "apply", "-f", str(manifest), "-n", app.namespace]
    assert manifest.read_text().endswith("relay: accepted bytes\n")
    install.assert_not_called()
