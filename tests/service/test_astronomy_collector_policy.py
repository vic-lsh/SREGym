"""Collector policy must be part of the accepted render, never a harness overlay."""


def test_collector_policy_reaches_deploy_through_accepted_manifest(monkeypatch, tmp_path):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import Mock
    from sregym.service.apps import astronomy_shop

    accepted = tmp_path / "accepted.yaml"
    accepted.write_text("synthetic collector policy")
    monkeypatch.setattr(
        astronomy_shop, "plan_for_app", lambda *a, **kw: nullcontext(SimpleNamespace(manifest_path=accepted))
    )
    helm = SimpleNamespace(install=Mock(), assert_if_deployed=Mock())
    monkeypatch.setattr(astronomy_shop, "Helm", helm)
    app = object.__new__(astronomy_shop.AstronomyShop)
    app.namespace = "astronomy-shop"
    app.helm_configs = {"namespace": app.namespace}
    app.kubectl = Mock()
    app.deploy()
    app.kubectl.exec_command_checked.assert_called_once_with(f"kubectl apply -f {accepted} -n astronomy-shop")
    helm.install.assert_not_called()
