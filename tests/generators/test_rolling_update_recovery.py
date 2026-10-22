"""rolling_update_misconfigured patches the live Deployment in place and undoes only its own change."""

import json

import pytest

import sregym.paths
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector


class _RecordingKubeCtl:
    def __init__(self):
        self.commands = []

    def exec_command(self, command):
        self.commands.append(command)
        return "OK\n"


@pytest.fixture
def injector(tmp_path, monkeypatch):
    monkeypatch.setattr(sregym.paths, "FAULT_SCRATCH_ROOT", tmp_path)
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    return VirtualizationFaultInjector("hotel-reservation", kubectl=_RecordingKubeCtl())


def _patch_of(command):
    assert "--type strategic --patch-file " in command
    with open(command.split("--patch-file ", 1)[1]) as f:
        return json.load(f)


def test_injection_patches_the_live_deployment_and_never_replaces_it(injector, tmp_path):
    injector._inject(
        "rolling_update_misconfigured",
        ["geo"],
        init_command="sleep 3600",
        max_unavailable="100%",
        max_surge="0%",
    )

    commands = injector.kubectl.commands
    assert commands[0].startswith("kubectl patch deployment geo -n hotel-reservation ")
    assert commands[1] == "kubectl rollout restart deployment geo -n hotel-reservation"
    assert not any(c.startswith(("kubectl apply", "kubectl delete")) for c in commands)
    assert commands[0].split("--patch-file ", 1)[1].startswith(str(tmp_path))

    patch = _patch_of(commands[0])
    assert "selector" not in patch["spec"]
    assert patch["spec"]["strategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {"maxUnavailable": "100%", "maxSurge": "0%"},
    }
    assert patch["spec"]["template"]["spec"] == {
        "initContainers": [{"name": "hang-init", "image": "busybox", "command": ["/bin/sh", "-c", "sleep 3600"]}]
    }


def test_recovery_removes_only_the_injected_init_container(injector):
    injector.recover_rolling_update_misconfigured(["geo"])

    commands = injector.kubectl.commands
    assert commands[1] == "kubectl rollout restart deployment geo -n hotel-reservation"
    assert not any(c.startswith(("kubectl apply", "kubectl delete")) for c in commands)
    patch = _patch_of(commands[0])
    assert patch["spec"]["template"]["spec"]["initContainers"] == [{"name": "hang-init", "$patch": "delete"}]
    assert patch["spec"]["strategy"]["rollingUpdate"] == {"maxUnavailable": "25%", "maxSurge": "25%"}
