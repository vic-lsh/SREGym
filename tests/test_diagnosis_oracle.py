"""Unit tests for DiagnosisOracle.compare_truth and safe_parse_solution.

Multi-diagnosis support: the agent may submit a list of candidate diagnoses,
and the oracle marks success if the ground truth matches any candidate.
"""

from __future__ import annotations

import pytest

from sregym.conductor.oracles.diagnosis_oracle import DiagnosisOracle


class _StubDiagnosisOracle(DiagnosisOracle):
    """Concrete subclass — `DiagnosisOracle` is abstract via `expect`."""

    def expect(self):
        return "expected"


@pytest.fixture
def oracle() -> _StubDiagnosisOracle:
    return _StubDiagnosisOracle(problem=None, namespace="test-ns")


# ---------------------------------------------------------------------------
# compare_truth — multi-diagnosis "any candidate matches" semantics
# ---------------------------------------------------------------------------


class TestCompareTruth:
    def test_str_vs_str_exact_match(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth("gt", "gt") is True

    def test_str_vs_str_mismatch(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth("gt", "wrong") is False

    def test_str_expectation_in_list_reality(self, oracle: _StubDiagnosisOracle):
        # Single ground truth contained in a list of submitted candidates → success.
        assert oracle.compare_truth("gt", ["gt", "other"]) is True

    def test_str_expectation_not_in_list_reality(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth("gt", ["wrong1", "wrong2"]) is False

    def test_list_subset_succeeds_with_extra_candidates(self, oracle: _StubDiagnosisOracle):
        # Reality has more candidates than expectation — should still pass.
        # The old length-equality gate blocked this; multi-diagnosis support drops that gate.
        assert oracle.compare_truth(["a", "b"], ["a", "b", "c"]) is True

    def test_list_exact_match(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth(["a", "b"], ["a", "b"]) is True

    def test_list_missing_required_item_fails(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth(["a", "b"], ["a", "c"]) is False

    def test_singleton_list_expectation_vs_str_reality(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth(["gt"], "gt") is True

    def test_singleton_list_expectation_vs_str_reality_mismatch(self, oracle: _StubDiagnosisOracle):
        assert oracle.compare_truth(["gt"], "wrong") is False


# ---------------------------------------------------------------------------
# safe_parse_solution
# ---------------------------------------------------------------------------


class TestSafeParseSolution:
    def test_plain_string_wrapped_into_singleton_list(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution("foo") == ["foo"]

    def test_actual_list_passes_through_as_strings(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution(["a", "b"]) == ["a", "b"]

    def test_string_with_list_literal_parses_to_list(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution('["a", "b"]') == ["a", "b"]

    def test_string_with_single_quote_list_literal(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution("['a', 'b']") == ["a", "b"]

    def test_non_string_non_list_returns_none(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution(42) is None

    def test_empty_string_falls_back_to_singleton(self, oracle: _StubDiagnosisOracle):
        # Empty input shouldn't crash; treat as a single empty candidate.
        assert oracle.safe_parse_solution("") == [""]

    def test_list_of_non_strings_coerced_to_strings(self, oracle: _StubDiagnosisOracle):
        assert oracle.safe_parse_solution([1, 2]) == ["1", "2"]
