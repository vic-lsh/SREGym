"""Unit tests for LLMAsAJudgeOracle multi-diagnosis support.

The oracle should accept either a single string or a list of candidate
diagnoses. Success = the LLM judge approves at least one candidate, with
short-circuit on the first match.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from sregym.conductor.oracles.llm_as_a_judge.judge import JudgmentResult
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle


@pytest.fixture
def oracle() -> LLMAsAJudgeOracle:
    # num_rounds=1 keeps assertion math simple; voting itself is unchanged.
    return LLMAsAJudgeOracle(problem=None, expected="GROUND_TRUTH", num_rounds=1)


def _judge_stub(approved_for: set[str]):
    """Return a stub for `LLMJudge.judge` that returns TRUE only for inputs in `approved_for`."""

    def _stub(self, solution: str, expectation: str) -> tuple[JudgmentResult, str]:
        if solution in approved_for:
            return JudgmentResult.TRUE, "matches"
        return JudgmentResult.FALSE, "no match"

    return _stub


class TestEvaluateSingleString:
    def test_string_pass(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub({"hit"}),
        ):
            result = oracle.evaluate("hit")
        assert result["success"] is True
        assert result["accuracy"] == 100.0
        assert result["matched_candidate"] == "hit"

    def test_string_fail(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub({"miss"}),
        ):
            result = oracle.evaluate("not the answer")
        assert result["success"] is False
        assert result["matched_candidate"] is None


class TestEvaluateCandidateList:
    def test_list_with_correct_candidate_succeeds(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub({"correct"}),
        ):
            result = oracle.evaluate(["wrong1", "correct", "wrong2"])
        assert result["success"] is True
        assert result["matched_candidate"] == "correct"
        # Accuracy mirrors the winning candidate (full credit on match).
        assert result["accuracy"] == 100.0
        assert result["candidates"] == ["wrong1", "correct", "wrong2"]
        assert result["num_candidates"] == 3

    def test_list_short_circuits_on_first_match(self, oracle: LLMAsAJudgeOracle):
        call_count = {"n": 0}

        def _counting_stub(self, solution, expectation):
            call_count["n"] += 1
            return JudgmentResult.TRUE, "always good"

        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_counting_stub,
        ):
            result = oracle.evaluate(["first", "second", "third"])

        # First candidate already matches → short-circuit, no further judge calls.
        assert result["success"] is True
        assert result["matched_candidate"] == "first"
        # num_rounds=1 → exactly 1 judge call total when first candidate wins.
        assert call_count["n"] == 1

    def test_list_evaluates_all_when_only_last_matches(self, oracle: LLMAsAJudgeOracle):
        call_count = {"n": 0}

        def _stub(self, solution, expectation):
            call_count["n"] += 1
            if solution == "winner":
                return JudgmentResult.TRUE, "yes"
            return JudgmentResult.FALSE, "no"

        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_stub,
        ):
            result = oracle.evaluate(["a", "b", "winner"])

        assert result["success"] is True
        assert result["matched_candidate"] == "winner"
        # num_rounds=1 × 3 candidates = 3 judge calls.
        assert call_count["n"] == 3

    def test_list_all_wrong_fails(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub(set()),  # nothing matches
        ):
            result = oracle.evaluate(["wrong1", "wrong2", "wrong3"])
        assert result["success"] is False
        assert result["matched_candidate"] is None
        assert len(result["per_candidate"]) == 3

    def test_per_candidate_results_recorded(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub({"correct"}),
        ):
            result = oracle.evaluate(["wrong", "correct"])

        assert "per_candidate" in result
        assert len(result["per_candidate"]) == 2
        assert result["per_candidate"][0]["success"] is False
        assert result["per_candidate"][1]["success"] is True

    def test_empty_list_returns_failure(self, oracle: LLMAsAJudgeOracle):
        result = oracle.evaluate([])
        assert result["success"] is False
        assert result["matched_candidate"] is None

    def test_list_of_one_string_behaves_like_string(self, oracle: LLMAsAJudgeOracle):
        with patch(
            "sregym.conductor.oracles.llm_as_a_judge.judge.LLMJudge.judge",
            new=_judge_stub({"hit"}),
        ):
            result = oracle.evaluate(["hit"])
        assert result["success"] is True
        assert result["matched_candidate"] == "hit"
