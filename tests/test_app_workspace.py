from pathlib import Path

from sregym.service.app_workspace import prepare_application_workspace
from sregym.service.source_deploy import _resolve_app_source_dir


def test_source_deploy_uses_prepared_workspace(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source" / "hotelReservation"
    source.mkdir(parents=True)
    (source / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    monkeypatch.setattr("sregym.service.app_workspace._target_microservices_root", lambda: tmp_path / "source")

    workspace = prepare_application_workspace(
        experiment_dir=tmp_path / "experiment",
        app_filter="hotel_reservation",
        resume=False,
    )
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(workspace))

    assert _resolve_app_source_dir("Hotel Reservation", source) == workspace


def test_optional_hotel_controller_is_in_initial_accepted_source(tmp_path, monkeypatch):
    import json
    import shutil
    import subprocess

    component = Path(__file__).resolve().parents[1] / "SREGym-applications/hotelReservation/optional/cleanup-controller"
    source = tmp_path / "source/hotelReservation"
    (source / "kubernetes").mkdir(parents=True)
    shutil.copytree(component, source / "optional/cleanup-controller")
    monkeypatch.setattr("sregym.service.app_workspace._target_microservices_root", lambda: tmp_path / "source")
    monkeypatch.setenv("SREGYM_HOTEL_CLEANUP_CONTROLLER", "1")
    workspace = prepare_application_workspace(
        experiment_dir=tmp_path / "experiment", app_filter="hotel_reservation", resume=False
    )
    content = subprocess.check_output(
        ["git", "-C", str(workspace), "show", "HEAD:kubernetes/cleanup-controller.yaml"], text=True
    )
    objects = [json.loads(doc) for doc in content.split("\n---\n")]
    assert len(objects) == 5
    assert all("platform.io/injected" not in obj["metadata"].get("labels", {}) for obj in objects)
