import asyncio
from pathlib import Path

import pytest
import yaml

from sregym.agent_registry import list_agents
from sregym.conductor import conductor_api
from sregym.conductor.conductor import AWAITING_FAULT_INJECTION, Conductor, ConductorConfig


class _Logger:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class _Problem:
    def __init__(self):
        self.injected = 0

    def inject_fault(self):
        self.injected += 1


def _conductor(*, defer_fault_injection: bool) -> Conductor:
    conductor = Conductor.__new__(Conductor)
    conductor.config = ConductorConfig(defer_fault_injection=defer_fault_injection)
    conductor.logger = _Logger()
    conductor.problem = _Problem()
    conductor.stage_sequence = [{"name": "diagnosis", "evaluation": lambda sol: None}]
    conductor.current_stage_index = 0
    conductor.waiting_for_agent = False
    conductor._evaluating = False
    conductor.fault_injected = False
    conductor.submission_stage = "setup"
    conductor.results = {}
    conductor.execution_start_time = 0.0
    conductor._submit_future = None
    return conductor


def test_registry_preserves_defer_fault_injection(tmp_path: Path) -> None:
    registry = tmp_path / "agents.yaml"
    registry.write_text(
        yaml.safe_dump({"agents": [{"name": "sdo_codex", "defer_fault_injection": True}, {"name": "codex"}]}),
        encoding="utf-8",
    )

    agents = list_agents(registry)

    assert agents["sdo_codex"].defer_fault_injection is True
    assert not agents["codex"].defer_fault_injection


def test_deferred_problem_waits_for_agent_before_injecting() -> None:
    conductor = _conductor(defer_fault_injection=True)

    conductor._begin_agent_stages()

    assert conductor.submission_stage == AWAITING_FAULT_INJECTION
    assert conductor.problem.injected == 0
    assert "fault_injected_at" not in conductor.results


def test_inject_deferred_fault_injects_once_and_starts_the_clock() -> None:
    conductor = _conductor(defer_fault_injection=True)
    conductor._begin_agent_stages()

    conductor.inject_deferred_fault()

    assert conductor.problem.injected == 1
    assert conductor.submission_stage == "diagnosis"
    assert conductor.results["fault_injected_at"] == pytest.approx(conductor.execution_start_time, abs=1.0)
    assert conductor.results["fault_injection_deferred_seconds"] >= 0
    with pytest.raises(RuntimeError):
        conductor.inject_deferred_fault()


def test_undeferred_problem_injects_immediately_and_records_injection_time() -> None:
    conductor = _conductor(defer_fault_injection=False)

    conductor._begin_agent_stages()

    assert conductor.problem.injected == 1
    assert conductor.submission_stage == "diagnosis"
    assert conductor.results["fault_injected_at"] == pytest.approx(conductor.execution_start_time, abs=1.0)


def test_submit_records_agent_submission_time_for_the_graded_stage(monkeypatch) -> None:
    conductor = _conductor(defer_fault_injection=False)
    conductor._begin_agent_stages()
    monkeypatch.setattr(conductor, "_submit_evaluate_and_advance", lambda sol, stage: None)

    asyncio.run(conductor.submit("root cause", received_at=123.5))

    assert conductor.results["diagnosis_submitted_at"] == 123.5


def test_inject_fault_endpoint_rejects_wrong_stage(monkeypatch) -> None:
    conductor = _conductor(defer_fault_injection=False)
    conductor._begin_agent_stages()
    monkeypatch.setattr(conductor_api, "_conductor", conductor)

    with pytest.raises(conductor_api.HTTPException) as error:
        asyncio.run(conductor_api.inject_fault())

    assert error.value.status_code == 409
