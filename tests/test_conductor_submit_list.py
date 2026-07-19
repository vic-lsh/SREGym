"""Direct-conductor plumbing tests for candidate-list submissions."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from sregym.conductor import conductor_api


def test_mcp_submission_preserves_list_type(monkeypatch):
    conductor = AsyncMock()
    conductor.submission_stage = "diagnosis"
    conductor.submit = AsyncMock(return_value={"Diagnosis": {"success": True}})
    monkeypatch.setattr(conductor_api, "_conductor", conductor)

    result = asyncio.run(conductor_api.submit_via_conductor.fn(["alpha", "beta"]))

    assert result["status"] == "200"
    conductor.submit.assert_awaited_once_with(["alpha", "beta"])


def test_mcp_submission_rejects_list_during_mitigation(monkeypatch):
    conductor = AsyncMock()
    conductor.submission_stage = "mitigation"
    conductor.submit = AsyncMock()
    monkeypatch.setattr(conductor_api, "_conductor", conductor)

    result = asyncio.run(conductor_api.submit_via_conductor.fn(["alpha"]))

    assert result["status"] == "error"
    conductor.submit.assert_not_awaited()
