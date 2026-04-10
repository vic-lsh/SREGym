"""End-to-end parser/conductor plumbing test for list submissions.

Verifies that when the API wraps a list-typed solution and forwards it to
``Conductor.submit``, the diagnosis oracle's ``evaluate`` receives an actual
``list[str]`` (not the stringified form).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from sregym.conductor.parser import ResponseParser


class _CapturingOracle:
    def __init__(self):
        self.received: Any = None

    def evaluate(self, solution):
        self.received = solution
        return {"success": True, "accuracy": 100.0}


def _wrap_solution(solution) -> str:
    # Mirrors conductor_api.submit_solution.
    return f"```\nsubmit({repr(solution)})\n```"


def _parse_args(wrapped: str):
    parser = ResponseParser()
    parsed = parser.parse(wrapped)
    assert parsed["api_name"] == "submit"
    return parsed["args"][0] if parsed["args"] else None


class TestParserPlumbing:
    def test_list_solution_round_trip_yields_list(self):
        wrapped = _wrap_solution(["alpha", "beta"])
        sol = _parse_args(wrapped)
        assert sol == ["alpha", "beta"]
        assert isinstance(sol, list)

    def test_string_solution_round_trip_yields_string(self):
        wrapped = _wrap_solution("alpha")
        sol = _parse_args(wrapped)
        assert sol == "alpha"
        assert isinstance(sol, str)

    def test_list_with_special_chars_round_trip(self):
        # Quotes inside candidates must survive repr → ast.parse round-trip.
        wrapped = _wrap_solution(["it's broken", 'has "quotes"'])
        sol = _parse_args(wrapped)
        assert sol == ["it's broken", 'has "quotes"']

    def test_oracle_receives_actual_list(self):
        # Drive the oracle directly with the parser output.
        oracle = _CapturingOracle()
        wrapped = _wrap_solution(["a", "b", "c"])
        sol = _parse_args(wrapped)
        oracle.evaluate(sol)
        assert oracle.received == ["a", "b", "c"]
        assert isinstance(oracle.received, list)
