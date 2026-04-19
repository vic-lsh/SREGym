"""Tests for the autonomous-submit API: ``POST /submit_stage`` and
``Conductor.submit_autonomous``.

Autonomous mode lets the cli_agent run a single session that grades
diagnosis and mitigation independently, without leaking the oracle's
verdict back to the agent. The agent is expected to self-verify via
kubectl; the benchmark still records per-stage results for offline
analysis. See ``sregym_agents/cli_agent/prompts/session_autonomous.j2``
for the agent-facing framing.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from sregym.conductor import conductor_api
from sregym.conductor.conductor import Conductor
from sregym.conductor.conductor_api import app


@pytest.fixture
def mock_conductor(monkeypatch):
    fake = MagicMock()
    fake.submit_autonomous = AsyncMock(return_value=None)
    monkeypatch.setattr(conductor_api, "_conductor", fake)
    return fake


class TestSubmitStageEndpoint:
    def test_diagnosis_submission_returns_neutral_ack(self, mock_conductor):
        """The response must not leak the oracle verdict — autonomous-mode
        agents are supposed to self-verify via the cluster, so any
        ``success``/``accuracy`` field here would undermine the design."""
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": "root-cause X", "stage": "diagnosis"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body == {"status": "recorded"}
        mock_conductor.submit_autonomous.assert_awaited_once_with(
            "diagnosis", "root-cause X"
        )

    def test_mitigation_submission_returns_neutral_ack(self, mock_conductor):
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": "restarted pods", "stage": "mitigation"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body == {"status": "recorded"}
        mock_conductor.submit_autonomous.assert_awaited_once_with(
            "mitigation", "restarted pods"
        )

    def test_list_solution_accepted(self, mock_conductor):
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": ["hypothesis A", "hypothesis B"], "stage": "diagnosis"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"status": "recorded"}
        mock_conductor.submit_autonomous.assert_awaited_once_with(
            "diagnosis", ["hypothesis A", "hypothesis B"]
        )

    def test_unknown_stage_rejected(self, mock_conductor):
        """Guard against typos: only ``diagnosis`` / ``mitigation`` are routable."""
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": "x", "stage": "recovery"},
        )
        assert resp.status_code == 400
        mock_conductor.submit_autonomous.assert_not_awaited()

    def test_missing_conductor_returns_400(self, monkeypatch):
        monkeypatch.setattr(conductor_api, "_conductor", None)
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": "x", "stage": "diagnosis"},
        )
        assert resp.status_code == 400

    def test_conductor_value_error_surfaces_as_400(self, mock_conductor):
        """If the oracle is not attached, the conductor raises ValueError —
        the endpoint must translate it to 400 so the agent sees a protocol
        error rather than a 500."""
        mock_conductor.submit_autonomous.side_effect = ValueError(
            "Diagnosis oracle is not attached"
        )
        client = TestClient(app)
        resp = client.post(
            "/submit_stage",
            json={"solution": "x", "stage": "diagnosis"},
        )
        assert resp.status_code == 400


class TestConductorSubmitAutonomous:
    """Dispatch test at the Conductor layer. We bypass the heavy __init__
    (kubeconfig/helm/proxy) via ``object.__new__`` since we only exercise
    the stage-dispatch logic."""

    def _make_conductor(
        self,
        *,
        diagnosis_oracle=None,
        mitigation_oracle=None,
    ) -> Conductor:
        c = object.__new__(Conductor)
        import logging

        c.logger = logging.getLogger("test.conductor")
        c.results = {}
        c.execution_start_time = 0.0
        problem = MagicMock()
        problem.diagnosis_oracle = diagnosis_oracle
        problem.mitigation_oracle = mitigation_oracle
        c.problem = problem
        return c

    def test_diagnosis_dispatched_to_diagnosis_oracle(self):
        diag = MagicMock()
        diag.evaluate = MagicMock(return_value={"success": True, "accuracy": 100.0})
        mit = MagicMock()
        mit.evaluate = MagicMock(return_value={"success": False})
        c = self._make_conductor(diagnosis_oracle=diag, mitigation_oracle=mit)

        asyncio.run(c.submit_autonomous("diagnosis", "root-cause"))

        diag.evaluate.assert_called_once_with("root-cause")
        mit.evaluate.assert_not_called()
        # Result recorded for offline analysis even though the agent won't see it.
        assert c.results["Diagnosis"] == {"success": True, "accuracy": 100.0}

    def test_mitigation_dispatched_to_mitigation_oracle(self):
        diag = MagicMock()
        diag.evaluate = MagicMock(return_value={"success": True})
        mit = MagicMock()
        mit.evaluate = MagicMock(return_value={"success": True})
        c = self._make_conductor(diagnosis_oracle=diag, mitigation_oracle=mit)

        asyncio.run(c.submit_autonomous("mitigation", "restarted deployment"))

        # Mitigation oracle ignores the solution string and only inspects
        # cluster state, matching the existing contract.
        mit.evaluate.assert_called_once_with()
        diag.evaluate.assert_not_called()
        assert c.results["Mitigation"] == {"success": True}

    def test_diagnosis_without_oracle_raises(self):
        c = self._make_conductor(diagnosis_oracle=None, mitigation_oracle=MagicMock())
        with pytest.raises(ValueError):
            asyncio.run(c.submit_autonomous("diagnosis", "x"))

    def test_mitigation_without_oracle_raises(self):
        c = self._make_conductor(diagnosis_oracle=MagicMock(), mitigation_oracle=None)
        with pytest.raises(ValueError):
            asyncio.run(c.submit_autonomous("mitigation", "x"))

    def test_unknown_stage_raises(self):
        c = self._make_conductor(
            diagnosis_oracle=MagicMock(), mitigation_oracle=MagicMock()
        )
        with pytest.raises(ValueError):
            asyncio.run(c.submit_autonomous("recovery", "x"))

    def test_submit_does_not_advance_stage_index(self):
        """Autonomous submissions must not touch the sequential state
        machine — the two stages are graded independently, and the normal
        stage-advancement logic is reserved for non-autonomous mode."""
        diag = MagicMock()
        diag.evaluate = MagicMock(return_value={"success": True})
        c = self._make_conductor(
            diagnosis_oracle=diag, mitigation_oracle=MagicMock()
        )
        c.current_stage_index = 0
        c.submission_stage = "diagnosis"

        asyncio.run(c.submit_autonomous("diagnosis", "x"))

        assert c.current_stage_index == 0
        assert c.submission_stage == "diagnosis"
