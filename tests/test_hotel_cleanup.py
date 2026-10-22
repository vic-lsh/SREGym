from unittest.mock import MagicMock

from sregym.service.apps import hotel_reservation
from sregym.service.apps.hotel_reservation import HotelReservation


def _app(pvs: str) -> tuple[HotelReservation, MagicMock]:
    app = HotelReservation.__new__(HotelReservation)
    app.namespace = "hotel-reservation"
    app.kubectl = MagicMock()
    app.kubectl.exec_command.return_value = pvs
    return app, app.kubectl


def test_cleanup_on_a_clean_cluster_neither_waits_nor_sleeps(monkeypatch) -> None:
    app, kubectl = _app("")
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)

    app.cleanup()

    assert sleeps == []
    assert not hasattr(hotel_reservation, "time")
    assert all("wait" not in call.args[0] for call in kubectl.exec_command.call_args_list)


def test_cleanup_waits_for_each_deleted_volume_instead_of_sleeping(monkeypatch) -> None:
    app, kubectl = _app("pv-a\npv-b")
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)

    app.cleanup()

    commands = [call.args[0] for call in kubectl.exec_command.call_args_list]
    assert [c for c in commands if "kubectl wait" in c] == [
        "kubectl wait --for=delete pv/pv-a --timeout=60s",
        "kubectl wait --for=delete pv/pv-b --timeout=60s",
    ]
    assert sleeps == []
