"""A submission that arrives while the previous stage is being graded must not be dropped.

The conductor used to answer it with 200 "already accepted" and discard it, so
a fast agent that fixed the fault and submitted its mitigation during
diagnosis grading ended with no mitigation verdict.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from sregym.conductor import conductor_api
from sregym.conductor.conductor import Conductor, SubmissionWhileEvaluating
from tests.test_deferred_fault_injection import _conductor


def test_conductor_refuses_rather_than_drops_a_submission_during_evaluation() -> None:
    conductor: Conductor = _conductor(defer_fault_injection=False)
    conductor.submission_stage = "diagnosis"
    conductor._evaluating = True

    with pytest.raises(SubmissionWhileEvaluating, match="diagnosis"):
        asyncio.run(conductor.submit("fixed it"))

    assert conductor.results == {}


def test_api_holds_the_submission_until_the_next_stage_opens(monkeypatch) -> None:
    fake = AsyncMock()
    fake.submission_stage = "diagnosis"
    calls = []

    async def submit(solution, received_at=None):
        calls.append((solution, received_at))
        if len(calls) < 3:
            raise SubmissionWhileEvaluating("diagnosis is being evaluated")
        return {"status": "ok"}

    fake.submit = submit
    monkeypatch.setattr(conductor_api, "_conductor", fake)
    ticks = iter(range(10_000))
    monkeypatch.setattr(conductor_api.time, "time", lambda: 100.0 + next(ticks))

    async def no_sleep(_seconds):
        fake.submission_stage = "mitigation"

    monkeypatch.setattr(conductor_api.asyncio, "sleep", no_sleep)

    response = TestClient(conductor_api.app).post("/submit", json={"solution": ""})

    assert response.status_code == 200, response.text
    assert len(calls) == 3
    # Accepted after waiting: stamped when the stage accepted it, like an agent that polled /status first.
    assert calls[-1][1] > calls[0][1]


def test_api_stamps_an_immediately_accepted_submission_on_arrival(monkeypatch) -> None:
    fake = AsyncMock()
    fake.submission_stage = "diagnosis"
    fake.submit = AsyncMock(return_value={"status": "ok"})
    monkeypatch.setattr(conductor_api, "_conductor", fake)
    monkeypatch.setattr(conductor_api.time, "time", lambda: 100.0)

    response = TestClient(conductor_api.app).post("/submit", json={"solution": "diag"})

    assert response.status_code == 200
    assert fake.submit.call_args.kwargs["received_at"] == 100.0
