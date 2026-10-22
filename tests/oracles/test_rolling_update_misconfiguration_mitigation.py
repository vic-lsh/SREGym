"""RollingUpdateMitigationOracle only observes: rollout complete and `hang-init` gone (no cluster)."""

import json
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.rolling_update_misconfiguration_mitigation import RollingUpdateMitigationOracle


class _Kubectl:
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.commands = []

    def exec_command(self, command):
        self.commands.append(command)
        return self.outputs.pop(0) if len(self.outputs) > 1 else self.outputs[0]


def _deployment(*, replicas=3, generation=4, observed=4, updated=3, total=3, available=3, init=()):
    return json.dumps(
        {
            "metadata": {"generation": generation},
            "spec": {
                "replicas": replicas,
                "template": {"spec": {"initContainers": [{"name": n} for n in init]}},
            },
            "status": {
                "observedGeneration": observed,
                "updatedReplicas": updated,
                "replicas": total,
                "availableReplicas": available,
            },
        }
    )


def _evaluate(monkeypatch, *outputs):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    kubectl = _Kubectl(outputs)
    problem = SimpleNamespace(namespace="hotel-reservation", kubectl=kubectl)
    return RollingUpdateMitigationOracle(problem, "custom-service").evaluate()["success"], kubectl


def test_completed_rollout_without_hang_init_passes_and_only_reads(monkeypatch):
    success, kubectl = _evaluate(monkeypatch, _deployment(init=("wait-for-db",)))
    assert success is True
    assert kubectl.commands == ["kubectl get deployment custom-service -n hotel-reservation -o json"]


def test_completed_rollout_with_hang_init_still_present_fails(monkeypatch):
    # slow_timeout: the init container eventually succeeds, so the rollout completes unfixed.
    assert _evaluate(monkeypatch, _deployment(init=("hang-init",)))[0] is False


@pytest.mark.parametrize(
    "status",
    [
        {"available": 0},  # drain_all
        {"total": 4},  # stuck canary: an old-template pod lingers
        {"updated": 1},
        {"observed": 3},  # controller has not seen the latest spec
    ],
)
def test_incomplete_rollout_fails(monkeypatch, status):
    assert _evaluate(monkeypatch, _deployment(**status))[0] is False


def test_empty_output_is_retried_then_fails(monkeypatch):
    success, kubectl = _evaluate(monkeypatch, "", "  ", "")
    assert success is False
    assert len(kubectl.commands) == 3
    assert _evaluate(monkeypatch, "", _deployment())[0] is True
