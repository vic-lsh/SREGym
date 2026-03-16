"""
Unit tests for clients.crucible.orchestrator._run_stage_loop.

Key behaviour under test:
- When the judge approves on any iteration, submit_to_benchmark is NOT called
  by the fallback path (the judge tool handles submission itself).
- When all iterations are exhausted without approval, submit_to_benchmark IS
  called exactly once with ("", stage) so the conductor can record TTM/TTL.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# The module under test
import clients.crucible.orchestrator as orch


def _make_agent_state(usage=None):
    return {"usage": usage or {"input_tokens": 1, "output_tokens": 1, "cached_input_tokens": 0}}


def _make_judge_state(verdict: str, usage=None):
    return {
        "verdict": verdict,
        "usage": usage or {"input_tokens": 1, "output_tokens": 1, "cached_input_tokens": 0},
    }


@pytest.fixture()
def shared_file(tmp_path: Path) -> Path:
    f = tmp_path / "session.md"
    f.write_text("# session\n")
    return f


@pytest.fixture()
def dummy_llm():
    llm = MagicMock()
    llm.model_name = "test-model"
    return llm


async def _run(shared_file, dummy_llm, judge_verdicts: list[str], max_iters: int = 3):
    """Helper: run _run_stage_loop with mocked agent/judge returning given verdicts."""
    agent_states = [_make_agent_state() for _ in judge_verdicts]
    judge_states = [_make_judge_state(v) for v in judge_verdicts]

    with (
        patch.object(orch, "_run_sre_agent", new=AsyncMock(side_effect=agent_states)),
        patch.object(orch, "_run_judge", new=AsyncMock(side_effect=judge_states)),
        patch.object(orch, "submit_to_benchmark", new=AsyncMock(return_value=(True, "ok", None))) as mock_submit,
    ):
        approved, _ = await orch._run_stage_loop(
            llm=dummy_llm,
            app_info={"app_name": "test", "namespace": "ns"},
            stage="mitigation",
            max_iters=max_iters,
            model_name="test-model",
            shared_file=shared_file,
            make_complete_tool=lambda sf, i: MagicMock(),
        )
        return approved, mock_submit


def test_fallback_submit_called_when_all_iterations_rejected(shared_file, dummy_llm):
    """submit_to_benchmark must be called once when all iterations are rejected."""
    approved, mock_submit = asyncio.run(
        _run(shared_file, dummy_llm, judge_verdicts=["REJECTED", "REJECTED", "REJECTED"])
    )

    assert not approved
    mock_submit.assert_awaited_once_with("", "mitigation")


def test_fallback_submit_not_called_when_approved(shared_file, dummy_llm):
    """submit_to_benchmark fallback must NOT be called when the judge approves."""
    approved, mock_submit = asyncio.run(
        _run(shared_file, dummy_llm, judge_verdicts=["APPROVED"])
    )

    assert approved
    mock_submit.assert_not_awaited()


def test_fallback_submit_not_called_on_late_approval(shared_file, dummy_llm):
    """submit_to_benchmark fallback must NOT be called when the judge approves on a later iteration."""
    approved, mock_submit = asyncio.run(
        _run(shared_file, dummy_llm, judge_verdicts=["REJECTED", "REJECTED", "APPROVED"])
    )

    assert approved
    mock_submit.assert_not_awaited()


def test_fallback_submit_called_once_regardless_of_max_iters(shared_file, dummy_llm):
    """Fallback submit is called exactly once even with max_iters=1."""
    approved, mock_submit = asyncio.run(
        _run(shared_file, dummy_llm, judge_verdicts=["REJECTED"], max_iters=1)
    )

    assert not approved
    mock_submit.assert_awaited_once_with("", "mitigation")
