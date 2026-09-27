"""Opt-in deferred diagnosis grading: the stage machine never waits for the LLM judge."""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import pytest

from sregym.conductor.conductor import Conductor, ConductorConfig


class BlockingJudge:
    """A diagnosis oracle that finishes only when the test releases it."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        self.graded: list[object] = []

    def evaluate(self, solution):
        self.started.set()
        assert self.release.wait(timeout=10), "test never released the judge"
        self.graded.append(solution)
        return {"success": True, "accuracy": 100.0}


class RecordingMitigationOracle:
    def __init__(self, judge: BlockingJudge) -> None:
        self.judge = judge
        self.judge_done_at_evaluation: bool | None = None

    def evaluate(self):
        self.judge_done_at_evaluation = bool(self.judge.graded)
        return {"success": True}


class FakeProblem:
    def __init__(self) -> None:
        self.diagnosis_oracle = BlockingJudge()
        self.mitigation_oracle = RecordingMitigationOracle(self.diagnosis_oracle)
        self.recovered = False

    def inject_fault(self) -> None:
        pass


def _conductor(*, defer_grading: bool, defer_cleanup: bool = False) -> tuple[Conductor, FakeProblem, list[str]]:
    conductor = Conductor.__new__(Conductor)
    conductor.config = ConductorConfig(defer_cleanup=defer_cleanup, defer_diagnosis_grading=defer_grading)
    conductor.logger = logging.getLogger("test.deferred_grading")
    problem = FakeProblem()
    conductor.problem = problem
    conductor.problem_id = "p"
    conductor.results = {}
    conductor.tasklist = ["diagnosis", "mitigation"]
    conductor._submit_future = None
    conductor._diagnosis_future = None
    conductor._cleanup_lock = threading.Lock()
    conductor._cleanup_timer = None
    conductor.submission_stage = None
    conductor._build_stage_sequence()
    conductor.fault_injected = True
    conductor.execution_start_time = time.time()
    events: list[str] = []

    def cleanup() -> None:
        # Teardown must see a graded diagnosis.
        events.append(f"cleanup:diagnosis={'Diagnosis' in conductor.results}")
        conductor.submission_stage = "done"

    conductor._cleanup_sync = cleanup
    conductor._start_cleanup_watchdog = lambda: events.append("watchdog")
    conductor._advance_to_next_stage(start_index=0)
    return conductor, problem, events


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.01)


def test_mitigation_is_accepted_and_graded_while_the_diagnosis_judge_is_still_running() -> None:
    conductor, problem, events = _conductor(defer_grading=True)

    asyncio.run(conductor.submit("mongo-geo-script ConfigMap is missing", received_at=100.0))

    # The stage machine advanced at once, without waiting for the judge.
    assert conductor.submission_stage == "mitigation"
    assert conductor.waiting_for_agent is True
    assert problem.diagnosis_oracle.started.wait(timeout=5)
    assert conductor.results["diagnosis_submitted_at"] == 100.0
    assert conductor.results["diagnosis_grading_deferred"] is True

    asyncio.run(conductor.submit("", received_at=130.0))
    _wait_until(lambda: "Mitigation" in conductor.results)

    assert problem.mitigation_oracle.judge_done_at_evaluation is False
    assert conductor.results["mitigation_submitted_at"] == 130.0
    # Teardown waits for the judge, so the published results always hold both verdicts.
    finisher = threading.Thread(target=conductor._finish_problem)
    finisher.start()
    time.sleep(0.1)
    assert events == []
    problem.diagnosis_oracle.release.set()
    finisher.join(timeout=5)
    conductor._submit_future.result(timeout=5)

    assert events == ["cleanup:diagnosis=True"]
    assert conductor.results["Diagnosis"]["success"] is True
    assert conductor.results["Diagnosis"]["submission"] == "mongo-geo-script ConfigMap is missing"
    assert "TTL" in conductor.results
    assert problem.diagnosis_oracle.graded == ["mongo-geo-script ConfigMap is missing"]


def test_deferred_cleanup_agents_reach_awaiting_cleanup_before_the_judge_finishes() -> None:
    conductor, problem, events = _conductor(defer_grading=True, defer_cleanup=True)

    asyncio.run(conductor.submit("diagnosis", received_at=1.0))
    asyncio.run(conductor.submit("", received_at=2.0))
    _wait_until(lambda: conductor.submission_stage == "awaiting_cleanup")

    assert "Diagnosis" not in conductor.results
    problem.diagnosis_oracle.release.set()
    conductor.force_cleanup()
    assert events == ["watchdog", "cleanup:diagnosis=True"]


def test_default_keeps_the_stage_machine_waiting_for_diagnosis_grading() -> None:
    conductor, problem, _events = _conductor(defer_grading=False)

    asyncio.run(conductor.submit("diagnosis", received_at=1.0))
    assert problem.diagnosis_oracle.started.wait(timeout=5)

    assert conductor.submission_stage == "diagnosis"
    assert conductor.waiting_for_agent is False
    assert "diagnosis_grading_deferred" not in conductor.results
    problem.diagnosis_oracle.release.set()
    _wait_until(lambda: conductor.submission_stage == "mitigation")


@pytest.mark.parametrize("raw", ["1", "true", "yes"])
def test_main_reads_the_opt_in_flag(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    from sregym.conductor.conductor import defer_diagnosis_grading_enabled

    monkeypatch.setenv("SREGYM_DEFER_DIAGNOSIS_GRADING", raw)
    assert defer_diagnosis_grading_enabled() is True
    monkeypatch.delenv("SREGYM_DEFER_DIAGNOSIS_GRADING")
    assert defer_diagnosis_grading_enabled() is False
