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
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(manifest))
    monkeypatch.setenv("SREGYM_DEPLOY_FROM_SOURCE", "1")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "synthetic-lane")
    monkeypatch.setattr(source._SUPPORTED_ADAPTERS["OpenTelemetry Demo Astronomy Shop"], "ensure_images_loaded", lambda *a, **kw: None)
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

    argv = shlex.split(app.kubectl.exec_command_checked.call_args.args[0])
    assert argv == ["kubectl", "apply", "-f", str(manifest), "-n", app.namespace]
    assert manifest.read_text().endswith("relay: accepted bytes\n")
    install.assert_not_called()


def test_astronomy_manifest_apply_failure_stops_before_readiness(monkeypatch, tmp_path):
    """A healthy old namespace cannot mask rejected or partially applied source."""
    import importlib
    import subprocess
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import Mock

    import pytest

    astronomy = importlib.import_module("sregym.service.apps.astronomy_shop")
    kubectl = importlib.import_module("sregym.service.kubectl")
    manifest = tmp_path / "accepted source.yaml"
    manifest.write_text("synthetic manifest")
    monkeypatch.setattr(
        astronomy, "plan_for_app", lambda *a, **kw: nullcontext(SimpleNamespace(manifest_path=manifest))
    )
    readiness = Mock()
    monkeypatch.setattr(astronomy.Helm, "assert_if_deployed", readiness)
    app = astronomy.AstronomyShop.__new__(astronomy.AstronomyShop)
    app.namespace = "astronomy-shop"
    app.helm_configs = {"namespace": app.namespace}
    app.kubectl = kubectl.KubeCtl.__new__(kubectl.KubeCtl)
    monkeypatch.setattr(app.kubectl, "create_namespace_if_not_exist", Mock())
    monkeypatch.setattr(app.kubectl, "get_node_architectures", lambda: {"amd64"})

    def reject(command, **kwargs):
        raise subprocess.CalledProcessError(
            1, command, output=b"configmap/first configured", stderr=b'Deployment "synthetic-api": field is immutable'
        )

    monkeypatch.setattr(kubectl.subprocess, "run", reject)
    with pytest.raises(RuntimeError, match='Deployment "synthetic-api": field is immutable') as error:
        app.deploy()
    assert str(manifest) in str(error.value)
    readiness.assert_not_called()
