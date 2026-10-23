"""No cluster/model client is created; verify the actual pilot problem wiring."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sregym.conductor.problems import source_memory_pilot as pilot


@pytest.fixture
def wired(monkeypatch):
    initial = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "search", "generation": 1},
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "search", "image": "app:v1"}],
                    "initContainers": [
                        {
                            "name": "wait-for-mongodb",
                            "image": "busybox:1.36",
                            "command": ["sh", "-c", "until nc -z mongodb-geo 27017; do sleep 2; done"],
                        }
                    ],
                }
            },
        },
        "status": {"observedGeneration": 1, "availableReplicas": 1, "updatedReplicas": 1},
    }
    current = copy.deepcopy(initial)
    patches = []

    def patch(_name, _ns, body):
        patches.append(body)
        current["spec"]["template"]["spec"]["initContainers"][0]["image"] = body["spec"]["template"]["spec"][
            "initContainers"
        ][0]["image"]

    kube = SimpleNamespace(get_deployment=lambda *_: current, patch_deployment=patch)
    monkeypatch.setattr(pilot, "KubeCtl", lambda: kube)
    monkeypatch.setattr(
        pilot, "create_app", lambda *_: SimpleNamespace(namespace="hotel-reservation", create_workload=Mock())
    )
    monkeypatch.setattr(pilot, "LLMAsAJudgeOracle", lambda **_: SimpleNamespace())
    monkeypatch.setattr(pilot.time, "sleep", lambda _: None)
    monkeypatch.setattr(pilot.NamedInitMitigationOracle, "settle_rollouts", lambda *_a, **_k: None)
    return initial, current, patches


def test_named_init_problem_injects_recovers_and_preserves_wait(wired):
    initial, current, patches = wired
    problem = pilot.NamedInitImage()
    problem.inject_fault()
    assert current["spec"]["template"]["spec"]["containers"] == initial["spec"]["template"]["spec"]["containers"]
    assert "registry.invalid" in current["spec"]["template"]["spec"]["initContainers"][0]["image"]
    assert not problem.mitigation_oracle.evaluate()["success"]
    problem.recover_fault()
    assert problem.mitigation_oracle.evaluate()["success"]
    current["spec"]["template"]["spec"]["initContainers"][0]["command"] = ["true"]
    assert not problem.mitigation_oracle.evaluate()["success"]
    assert len(patches) == 2


def test_healthy_tracing_performs_no_mutation(wired, monkeypatch):
    _, _, patches = wired
    sleeps = []
    monkeypatch.setattr(pilot.time, "sleep", sleeps.append)
    problem = pilot.TracingHealthyObservation()
    problem.inject_fault()
    assert sleeps == [120]
    assert patches == []
    problem.recover_fault()


def test_port_factories_select_expected_binding_and_named_bad_target(monkeypatch):
    seen = []
    monkeypatch.setattr(pilot, "ObservedTargetPortMisconfig", lambda **kwargs: seen.append(kwargs))
    pilot.sidecar_target_port()
    pilot.named_target_port()
    assert seen[0]["faulty_service"] == "reservation"
    assert seen[0]["correct_port"] == 8083
    assert seen[1]["faulty_service"] == "geo"
    assert isinstance(seen[1]["bad_port"], str)
