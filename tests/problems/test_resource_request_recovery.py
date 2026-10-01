"""Resource-request recovery applies the backup the injector wrote, from the same scratch directory."""

from types import SimpleNamespace

from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.paths import fault_scratch_path


def test_recover_applies_the_backup_written_by_the_injector(monkeypatch):
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "scratch-test")
    commands = []
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.namespace = "hotel-reservation"
    injector.kubectl = SimpleNamespace(exec_command=lambda command: commands.append(command) or "")

    injector.recover_resource_request(["user"])

    assert commands[-1] == f"kubectl apply -f {fault_scratch_path('user_modified.yaml')} -n hotel-reservation"
