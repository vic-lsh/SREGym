"""Hotel's bootstrap configuration is application source, not harness state."""

import shutil
import subprocess

import pytest
import yaml

from sregym.paths import TARGET_MICROSERVICES
from sregym.service import source_deploy


def test_source_plan_requires_accepted_render_before_any_build(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "offline")
    monkeypatch.delenv("SREGYM_ACCEPTED_RENDER", raising=False)

    def forbidden(**kwargs):
        pytest.fail("build happened before accepted-render validation")

    monkeypatch.setattr(source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"], "ensure_images_loaded", forbidden)
    with pytest.raises(source_deploy.SourceDeployUnsupportedError, match="accepted revision render"):
        with source_deploy.plan_for_app(SimpleNamespace(name="Hotel Reservation")):
            pytest.fail("missing accepted render admitted")


def test_pinned_hotel_source_renders_its_mounted_bootstrap_scripts(monkeypatch):
    """Real app -> real staging -> kubectl Kustomize, without Docker or a cluster."""
    source = TARGET_MICROSERVICES / "hotelReservation"
    assert source.is_dir(), "initialize the pinned SREGym-applications submodule"
    assert shutil.which("kubectl"), "kubectl is required to verify application source"
    output = subprocess.run(
        ["kubectl", "kustomize", str(source / "kubernetes")], check=True, capture_output=True, text=True
    ).stdout
    objects = list(yaml.safe_load_all(output))
    configs = {obj["metadata"]["name"]: obj for obj in objects if obj["kind"] == "ConfigMap"}
    for component in ("geo", "rate"):
        name, key = f"mongo-{component}-script", f"k8s-{component}-mongo.sh"
        script = source / "kubernetes" / "scripts" / key
        assert configs[name]["data"] == {key: script.read_text()}
        deployment = next(
            obj for obj in objects if obj["kind"] == "Deployment" and obj["metadata"]["name"] == f"mongodb-{component}"
        )
        assert any(
            v.get("configMap", {}).get("name") == name for v in deployment["spec"]["template"]["spec"]["volumes"]
        )


@pytest.mark.parametrize("source_build", [False, True])
def test_hotel_deploy_uses_source_configmaps_without_out_of_band_creation(monkeypatch, tmp_path, source_build):
    import contextlib
    from types import SimpleNamespace
    from unittest.mock import Mock

    from sregym.service.apps import hotel_reservation

    app = hotel_reservation.HotelReservation.__new__(hotel_reservation.HotelReservation)
    app.namespace = "hotel-reservation"
    app.k8s_deploy_path = str(tmp_path / "kubernetes")
    app.logger = Mock()
    app.kubectl = Mock()
    app.create_namespace = Mock()
    app.failure_scripts = hotel_reservation.FailureScripts.ABSENT
    monkeypatch.setattr(hotel_reservation, "source_deploy_enabled", lambda: source_build)
    monkeypatch.setattr(
        hotel_reservation,
        "plan_for_app",
        lambda app: contextlib.nullcontext(SimpleNamespace(manifest_path=app.k8s_deploy_path)),
    )
    app.deploy()
    app.kubectl.create_or_update_configmap.assert_not_called()
    app.kubectl.exec_command.assert_called_once_with(
        f"kubectl apply -{'f' if source_build else 'k'} {app.k8s_deploy_path} -n hotel-reservation"
    )
    app.kubectl.wait_for_ready.assert_called_once_with("hotel-reservation")
