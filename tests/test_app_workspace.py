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
