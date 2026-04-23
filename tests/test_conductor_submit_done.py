"""Tests for the autonomous-mode `submit_done` flow.

`submit_done` (new) freezes TTL at call time, runs the deferred diagnosis judge,
returns either a neutral payload or rich feedback depending on environment,
and rejects any further `submit_diagnosis` / `submit_mitigation` calls.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from sregym.conductor import conductor_api
from sregym.conductor.conductor import Conductor
from sregym.conductor.conductor_api import app


def _make_conductor(with_mitigation: bool = False) -> Conductor:
    """Construct a Conductor with just enough state to exercise submit_done.

    Avoids the heavy real __init__ (kubeconfig, helm) by bypassing it and
    setting only the attributes the done-flow touches.
    """
    c = Conductor.__new__(Conductor)
    c.logger = MagicMock()
    c.results = {}
    c.diagnosis_submissions = []
    c.autonomous_done = False
    c._ttl_stamped_at = None
    c.execution_start_time = time.time() - 10.0  # 10s elapsed
    c.problem = MagicMock()
    c.problem.diagnosis_oracle = MagicMock()
    c.problem.diagnosis_oracle.checkpoint = ["jaeger: replicas=2 + RWO PVC deadlock"]
    c.problem.diagnosis_oracle.evaluate = MagicMock(
        return_value={
            "success": True,
            "accuracy": 100.0,
            "judgment": "True",
            "reasoning": "Agent correctly identified jaeger RWO/anti-affinity deadlock.",
            "matched_candidate": "jaeger: replicas=2 + RWO PVC deadlock",
            "matched_candidate_index": 0,
        }
    )
    if with_mitigation:
        c.results["Mitigation"] = {"success": True, "accuracy": 100.0}
        c.results["TTM"] = 5.0
    return c


class TestSubmitDoneEndpoint:
    def test_no_conductor_returns_400(self, monkeypatch):
        monkeypatch.setattr(conductor_api, "_conductor", None)
        client = TestClient(app)
        assert client.post("/submit_done").status_code == 400

    def test_stamps_ttl_and_runs_judge(self, monkeypatch):
        c = _make_conductor()
        c.diagnosis_submissions.append("jaeger pending due to RWO PVC + anti-affinity")
        monkeypatch.setattr(conductor_api, "_conductor", c)
        monkeypatch.delenv("SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK", raising=False)

        resp = TestClient(app).post("/submit_done")
        assert resp.status_code == 200
        body = resp.json()

        assert body["status"] == "done"
        assert body["ttl"] is not None and body["ttl"] >= 10.0
        assert body["num_diagnosis_submissions"] == 1
        assert "diagnosis" not in body
        assert "mitigation" not in body
        assert "ground_truth_diagnosis" not in body
        assert c.autonomous_done is True
        c.problem.diagnosis_oracle.evaluate.assert_called_once()

    def test_returns_rich_feedback_when_enabled(self, monkeypatch):
        c = _make_conductor()
        c.diagnosis_submissions.append("jaeger pending due to RWO PVC + anti-affinity")
        monkeypatch.setattr(conductor_api, "_conductor", c)
        monkeypatch.setenv("SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK", "1")

        resp = TestClient(app).post("/submit_done")
        assert resp.status_code == 200
        body = resp.json()

        assert body["diagnosis"]["success"] is True
        assert "reasoning" in body["diagnosis"]
        assert body["ground_truth_diagnosis"] == ["jaeger: replicas=2 + RWO PVC deadlock"]

    def test_is_idempotent(self, monkeypatch):
        c = _make_conductor()
        c.diagnosis_submissions.append("some diagnosis")
        monkeypatch.setattr(conductor_api, "_conductor", c)

        client = TestClient(app)
        first = client.post("/submit_done").json()
        second = client.post("/submit_done").json()
        assert first == second
        # Judge should only be invoked once even on the second done call.
        c.problem.diagnosis_oracle.evaluate.assert_called_once()

    def test_includes_mitigation_when_present(self, monkeypatch):
        c = _make_conductor(with_mitigation=True)
        c.diagnosis_submissions.append("x")
        monkeypatch.setattr(conductor_api, "_conductor", c)
        monkeypatch.setenv("SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK", "1")
        body = TestClient(app).post("/submit_done").json()
        assert body["mitigation"]["success"] is True
        assert body["ttm"] == 5.0

    def test_handles_no_submissions_gracefully(self, monkeypatch):
        c = _make_conductor()
        # No diagnosis_submissions collected.
        monkeypatch.setattr(conductor_api, "_conductor", c)
        monkeypatch.delenv("SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK", raising=False)
        body = TestClient(app).post("/submit_done").json()
        assert body["status"] == "done"
        assert body["num_diagnosis_submissions"] == 0
        assert "diagnosis" not in body
        # No evaluation attempted.
        c.problem.diagnosis_oracle.evaluate.assert_not_called()


class TestPostDoneSubmissionsRejected:
    def test_submit_diagnosis_rejected_after_done(self, monkeypatch):
        c = _make_conductor()
        c.autonomous_done = True
        monkeypatch.setattr(conductor_api, "_conductor", c)
        resp = TestClient(app).post("/submit_diagnosis", json={"solution": "late finding"})
        assert resp.status_code == 409
        assert c.diagnosis_submissions == []

    def test_submit_mitigation_rejected_after_done(self, monkeypatch):
        c = _make_conductor()
        c.autonomous_done = True
        monkeypatch.setattr(conductor_api, "_conductor", c)
        resp = TestClient(app).post("/submit_mitigation", json={"solution": "late fix"})
        assert resp.status_code == 409


class TestConductorSubmitDoneMethod:
    """Exercise Conductor.submit_done() directly (bypassing HTTP)."""

    def test_ttl_frozen_at_call_time_not_judge_time(self):
        c = _make_conductor()
        c.diagnosis_submissions.append("d")

        # Make the judge slow so we can prove TTL uses the earlier stamp.
        def slow_eval(_sol):
            time.sleep(0.2)
            return {"success": True, "accuracy": 100.0}

        c.problem.diagnosis_oracle.evaluate = slow_eval
        t_call = time.time() - c.execution_start_time
        payload = c.submit_done()
        # TTL should reflect time at submit_done entry, not after the slow judge.
        assert payload["ttl"] == pytest.approx(t_call, abs=0.15)
        assert "diagnosis" not in payload

    def test_second_call_short_circuits(self):
        c = _make_conductor()
        c.diagnosis_submissions.append("d")
        c.submit_done()
        c.problem.diagnosis_oracle.evaluate.reset_mock()
        c.submit_done()
        c.problem.diagnosis_oracle.evaluate.assert_not_called()
