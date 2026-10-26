"""The harness consumes app-owned collector CPU configuration."""

from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from sregym.paths import TARGET_MICROSERVICES
from sregym.service.apps.astronomy_shop import AstronomyShop


def test_deploy_loads_app_owned_collector_policy():
    app = object.__new__(AstronomyShop)
    app.namespace = "astronomy-shop"
    app.helm_configs = {"namespace": "astronomy-shop"}
    app.kubectl = Mock()
    with (
        patch("sregym.service.apps.astronomy_shop.source_deploy_enabled", return_value=False),
        patch("sregym.service.apps.astronomy_shop.Helm", autospec=True) as helm,
    ):
        app.deploy()
    args = helm.install.call_args.kwargs["extra_args"]
    policy = Path(args[args.index("--values") + 1])
    assert policy == TARGET_MICROSERVICES / "astronomy-shop-overrides.yaml"
    collector = yaml.safe_load(policy.read_text())["opentelemetry-collector"]
    assert collector["extraEnvs"] == [{"name": "GOMAXPROCS", "value": "2"}]
    assert "resources" not in collector
    assert collector["presets"]["hostMetrics"]["enabled"] is False
    assert "prometheus.enabled=false" in args
    assert "components.load-generator.envOverrides[0].value=false" in args
