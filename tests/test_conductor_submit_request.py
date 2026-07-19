"""Tests for the SubmitRequest schema and the /submit endpoint cap.

Multi-diagnosis support: ``SubmitRequest.solution`` accepts ``str`` or
``list[str]``; the endpoint enforces a candidate cap and rejects empty lists.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from sregym.conductor import conductor_api
from sregym.conductor.conductor_api import SubmitRequest, app
from sregym.conductor.constants import MAX_DIAGNOSIS_CANDIDATES

# ---------------------------------------------------------------------------
# Pydantic schema
# ---------------------------------------------------------------------------


class TestSubmitRequestSchema:
    def test_string_solution_validates(self):
        req = SubmitRequest(solution="foo")
        assert req.solution == "foo"

    def test_list_of_strings_validates(self):
        req = SubmitRequest(solution=["a", "b"])
        assert req.solution == ["a", "b"]

    def test_list_of_ints_rejected(self):
        with pytest.raises(ValidationError):
            SubmitRequest(solution=[1, 2])

    def test_none_rejected(self):
        with pytest.raises(ValidationError):
            SubmitRequest(solution=None)


# ---------------------------------------------------------------------------
# /submit endpoint — cap + empty-list rejection
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conductor(monkeypatch):
    """Inject a fake conductor that records the typed answer it receives."""
    fake = AsyncMock()
    fake.submission_stage = "diagnosis"
    fake.submit = AsyncMock(return_value={"Diagnosis": {"success": True}})

    monkeypatch.setattr(conductor_api, "_conductor", fake)
    return fake


class TestSubmitEndpoint:
    def test_string_solution_accepted(self, mock_conductor):
        client = TestClient(app)
        resp = client.post("/submit", json={"solution": "single answer"})
        assert resp.status_code == 200, resp.text
        mock_conductor.submit.assert_awaited_once()
        assert mock_conductor.submit.call_args.args[0] == "single answer"

    def test_list_solution_accepted(self, mock_conductor):
        client = TestClient(app)
        resp = client.post("/submit", json={"solution": ["cand A", "cand B"]})
        assert resp.status_code == 200, resp.text
        assert mock_conductor.submit.call_args.args[0] == ["cand A", "cand B"]

    def test_empty_list_rejected(self, mock_conductor):
        client = TestClient(app)
        resp = client.post("/submit", json={"solution": []})
        assert resp.status_code == 400
        mock_conductor.submit.assert_not_awaited()

    def test_oversize_list_rejected(self, mock_conductor):
        client = TestClient(app)
        too_many = [f"cand{i}" for i in range(MAX_DIAGNOSIS_CANDIDATES + 1)]
        resp = client.post("/submit", json={"solution": too_many})
        assert resp.status_code == 400
        mock_conductor.submit.assert_not_awaited()

    def test_max_size_list_accepted(self, mock_conductor):
        client = TestClient(app)
        exact = [f"cand{i}" for i in range(MAX_DIAGNOSIS_CANDIDATES)]
        resp = client.post("/submit", json={"solution": exact})
        assert resp.status_code == 200, resp.text
        mock_conductor.submit.assert_awaited_once()

    def test_list_rejected_during_mitigation(self, mock_conductor):
        mock_conductor.submission_stage = "mitigation"
        client = TestClient(app)
        resp = client.post("/submit", json={"solution": ["candidate"]})
        assert resp.status_code == 400
        mock_conductor.submit.assert_not_awaited()
