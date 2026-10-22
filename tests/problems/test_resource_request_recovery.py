"""Resource-request recovery applies the backup the injector wrote, from the same scratch directory."""

from types import SimpleNamespace

import sregym.paths
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector


def test_recover_applies_the_backup_written_by_the_injector(monkeypatch, tmp_path):
    monkeypatch.setattr(sregym.paths, "FAULT_SCRATCH_ROOT", tmp_path)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "scratch-test")
    commands = []
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.namespace = "hotel-reservation"
    injector.kubectl = SimpleNamespace(exec_command=lambda command: commands.append(command) or "")

    backup = injector._save_backup("resource_request", "user", {"kind": "Deployment", "metadata": {"name": "user"}})

    injector.recover_resource_request(["user"])

    assert backup == str(tmp_path / "sregym-scratch-test" / "hotel-reservation_user_resource_request_backup.yaml")
    assert commands[-1] == f"kubectl apply -f {backup} -n hotel-reservation"
