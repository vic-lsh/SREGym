"""Hotel's bootstrap configuration is application source, not harness state."""

import shutil
import subprocess

import pytest
import yaml

from sregym.paths import TARGET_MICROSERVICES
from sregym.service import source_deploy


@pytest.mark.parametrize("filename", ["kustomization.yaml", "kustomization.yml", "Kustomization"])
def test_staging_preserves_declared_kustomization_and_file_generators(tmp_path, monkeypatch, filename):
    source = tmp_path / "hotel"
    manifests = source / "kubernetes"
    manifests.mkdir(parents=True)
    declaration = (
        "configMapGenerator:\n- name: startup\n  files: [init.sh]\ngeneratorOptions: {disableNameSuffixHash: true}\n"
    )
    (manifests / filename).write_text(declaration)
    (manifests / "init.sh").write_text("echo source-owned\n")
    # An unselected YAML file must not become a deployed resource by scanning.
    (manifests / "unused.yaml").write_text("not a selected Kubernetes object\n")
    adapter = source_deploy._HotelReservationAdapter()
    monkeypatch.setattr(adapter, "_source_dir", lambda: source)
    monkeypatch.setattr(adapter, "ensure_images_loaded", lambda **kwargs: None)
    with adapter.plan(cluster_name="offline", node_architectures=set()) as plan:
        assert (plan.manifest_path / filename).read_text() == declaration
        assert (plan.manifest_path / "init.sh").read_text() == "echo source-owned\n"
        assert len(list(plan.manifest_path.glob("*ustomization*"))) == 1
    assert (manifests / filename).read_text() == declaration


def test_pinned_hotel_source_renders_its_mounted_bootstrap_scripts(monkeypatch):
    """Real app -> real staging -> kubectl Kustomize, without Docker or a cluster."""
    source = TARGET_MICROSERVICES / "hotelReservation"
    assert source.is_dir(), "initialize the pinned SREGym-applications submodule"
    assert shutil.which("kubectl"), "kubectl is required to verify application source"
    adapter = source_deploy._HotelReservationAdapter()
    monkeypatch.setattr(adapter, "_source_dir", lambda: source)
    monkeypatch.setattr(adapter, "ensure_images_loaded", lambda **kwargs: None)
    with adapter.plan(cluster_name="offline", node_architectures=set()) as plan:
        output = subprocess.run(
            ["kubectl", "kustomize", str(plan.manifest_path)], check=True, capture_output=True, text=True
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
    app.kubectl.exec_command.assert_called_once_with(f"kubectl apply -k {app.k8s_deploy_path} -n hotel-reservation")
    app.kubectl.wait_for_ready.assert_called_once_with("hotel-reservation")
