"""Tests for candidate diagnosis grading and majority voting."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from sregym.conductor.oracles.llm_as_a_judge.judge import JudgmentResult
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle


@pytest.fixture
def oracle() -> LLMAsAJudgeOracle:
    return LLMAsAJudgeOracle(problem=None, expected="GROUND_TRUTH", num_rounds=1)


def _report(success: bool):
    return SimpleNamespace(
        verdict=JudgmentResult.TRUE if success else JudgmentResult.FALSE,
        reasoning="matches" if success else "no match",
        composite_score=1.0 if success else 0.0,
        dimensions=[],
    )


def _judge_stub(approved_for: set[str]):
    def _stub(self, solution: str, expectation: str):
        return _report(solution in approved_for)

    return _stub


class TestEvaluateSingleString:
    def test_string_pass(self, oracle):
        with patch.object(type(oracle.judge), "judge_detailed", new=_judge_stub({"hit"})):
            result = oracle.evaluate("hit")
        assert result["success"] is True
        assert result["accuracy"] == 100.0
        assert result["matched_candidate"] == "hit"

    def test_string_fail(self, oracle):
        with patch.object(type(oracle.judge), "judge_detailed", new=_judge_stub(set())):
            result = oracle.evaluate("not the answer")
        assert result["success"] is False
        assert result["matched_candidate"] is None


class TestEvaluateCandidateList:
    def test_correct_candidate_succeeds_and_short_circuits(self, oracle):
        calls: list[str] = []

        def _stub(self, solution, expectation):
            calls.append(solution)
            return _report(solution == "correct")

        with patch.object(type(oracle.judge), "judge_detailed", new=_stub):
            result = oracle.evaluate(["wrong", "correct", "not-run"])

        assert result["success"] is True
        assert result["matched_candidate"] == "correct"
        assert result["matched_candidate_index"] == 1
        assert result["candidates"] == ["wrong", "correct", "not-run"]
        assert calls == ["wrong", "correct"]

    def test_all_wrong_candidates_fail(self, oracle):
        with patch.object(type(oracle.judge), "judge_detailed", new=_judge_stub(set())):
            result = oracle.evaluate(["wrong1", "wrong2"])
        assert result["success"] is False
        assert result["matched_candidate"] is None
        assert len(result["per_candidate"]) == 2

    def test_empty_list_fails_without_judging(self, oracle):
        with patch.object(type(oracle.judge), "judge_detailed") as judge:
            result = oracle.evaluate([])
        assert result["success"] is False
        assert result["matched_candidate"] is None
        judge.assert_not_called()


def test_majority_vote_uses_upstream_detailed_reports():
    oracle = LLMAsAJudgeOracle(problem=None, expected="GROUND_TRUTH", num_rounds=3)
    verdicts = iter([True, False, True])

    def _stub(self, solution, expectation):
        return _report(next(verdicts))

    with (
        patch.object(type(oracle.judge), "judge_detailed", new=_stub),
        patch.object(type(oracle.judge), "backend", new=property(lambda self: None)),
    ):
        result = oracle.evaluate("candidate")

    assert result["success"] is True
    assert result["vote_count"] == "2/3"
    assert len(result["round_details"]) == 3


def test_invalid_round_count_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        LLMAsAJudgeOracle(problem=None, expected="GROUND_TRUTH", num_rounds=0)
