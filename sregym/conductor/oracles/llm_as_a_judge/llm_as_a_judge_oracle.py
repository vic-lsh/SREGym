"""LLM diagnosis grading with bounded candidates and majority voting."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.judge import DiagnosisJudge, JudgmentResult


class LLMAsAJudgeOracle(Oracle):
    """Evaluate one or more diagnoses using upstream's checklist judge.

    The upstream ``DiagnosisJudge`` remains the source of individual verdicts.
    This wrapper preserves the fork's higher-level behavior: multiple candidate
    diagnoses and configurable majority voting across independent judge calls.
    """

    def __init__(
        self,
        problem,
        expected: str,
        provider: str | None = None,
        model_name: str | None = None,
        url: str | None = None,
        api_key: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        num_rounds: int | None = None,
        voting_temperature: float | None = None,
    ):
        super().__init__(problem)
        self.expected = expected or ""
        self.num_rounds = num_rounds if num_rounds is not None else int(os.getenv("JUDGE_NUM_ROUNDS", "3"))
        if self.num_rounds < 1:
            raise ValueError("num_rounds must be at least 1")
        self.voting_temperature = (
            voting_temperature
            if voting_temperature is not None
            else float(os.getenv("JUDGE_VOTING_TEMPERATURE", "0.7"))
        )
        self.judge = DiagnosisJudge(
            provider=provider,
            model_name=model_name,
            url=url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    @staticmethod
    def _to_candidates(solution: Any) -> list[str]:
        if isinstance(solution, str):
            return [solution]
        if isinstance(solution, list):
            return [item if isinstance(item, str) else str(item) for item in solution]
        return [str(solution)]

    @staticmethod
    def _report_details(report: Any, round_index: int) -> dict[str, Any]:
        verdict = report.verdict
        return {
            "round": round_index,
            "judgment": verdict.value if verdict is not None else None,
            "reasoning": report.reasoning,
            "composite_score": report.composite_score,
            "dimensions": {
                dim.dimension_id: {
                    "name": dim.dimension_name,
                    "score": dim.score,
                }
                for dim in report.dimensions
            },
            "checklist": [
                {
                    "id": question.question_id,
                    "answer": "Yes" if question.answer else "No",
                    "evidence": question.evidence,
                    "confidence": question.confidence,
                }
                for dimension in report.dimensions
                for question in dimension.questions
            ],
        }

    def _run_single_round(self, round_index: int, candidate: str) -> dict[str, Any]:
        try:
            report = self.judge.judge_detailed(solution=candidate, expectation=self.expected)
            return self._report_details(report, round_index)
        except Exception as exc:
            return {"round": round_index, "error": str(exc)}

    def _evaluate_single_candidate(self, candidate: str) -> dict[str, Any]:
        with ThreadPoolExecutor(max_workers=self.num_rounds) as executor:
            futures = [executor.submit(self._run_single_round, index, candidate) for index in range(self.num_rounds)]
            rounds = [future.result() for future in as_completed(futures)]
        rounds.sort(key=lambda item: item["round"])

        true_rounds = [item for item in rounds if item.get("judgment") == JudgmentResult.TRUE.value]
        false_rounds = [item for item in rounds if item.get("judgment") == JudgmentResult.FALSE.value]
        valid_rounds = true_rounds + false_rounds
        unavailable_rounds = [item for item in rounds if item.get("judgment") is None and "error" not in item]

        if not valid_rounds:
            errors = [item["error"] for item in rounds if "error" in item]
            return {
                "judgment": None if unavailable_rounds else "Error",
                "reasoning": "LLM judge is not initialized" if unavailable_rounds else "; ".join(errors),
                "success": None if unavailable_rounds else False,
                "accuracy": None if unavailable_rounds else 0.0,
                "num_rounds": self.num_rounds,
                "round_details": rounds,
                "error": "; ".join(errors) if errors else None,
            }

        success = len(true_rounds) > len(false_rounds)
        winning_rounds = true_rounds if success else false_rounds
        representative = winning_rounds[0]
        average_score = sum(float(item.get("composite_score", 0.0)) for item in valid_rounds) / len(valid_rounds)
        return {
            "judgment": JudgmentResult.TRUE.value if success else JudgmentResult.FALSE.value,
            "reasoning": representative.get("reasoning", ""),
            "success": success,
            "accuracy": round(average_score * 100.0, 2),
            "composite_score": average_score,
            "dimensions": representative.get("dimensions", {}),
            "checklist": representative.get("checklist", []),
            "vote_count": f"{len(true_rounds)}/{len(valid_rounds)}",
            "num_rounds": self.num_rounds,
            "round_details": rounds,
        }

    def evaluate(self, solution: Any, duration: float | None = None) -> dict[str, Any]:
        """Evaluate candidates, succeeding when any candidate wins its vote."""
        del duration
        candidates = self._to_candidates(solution)
        if not candidates:
            return {
                "judgment": JudgmentResult.FALSE.value,
                "reasoning": "Submission contained no candidate diagnoses.",
                "success": False,
                "accuracy": 0.0,
                "num_rounds": self.num_rounds,
                "candidates": [],
                "num_candidates": 0,
                "matched_candidate": None,
                "matched_candidate_index": None,
                "per_candidate": [],
            }

        backend = self.judge.backend if self.num_rounds > 1 else None
        original_temperature = getattr(backend, "temperature", None) if backend is not None else None
        if backend is not None and hasattr(backend, "temperature"):
            backend.temperature = self.voting_temperature

        try:
            per_candidate: list[dict[str, Any]] = []
            matched_index: int | None = None
            for index, candidate in enumerate(candidates):
                candidate_result = self._evaluate_single_candidate(candidate)
                candidate_result["candidate"] = candidate
                per_candidate.append(candidate_result)
                if candidate_result.get("success") is True:
                    matched_index = index
                    break

            final = per_candidate[matched_index] if matched_index is not None else per_candidate[-1]
            result = dict(final)
            result.update(
                {
                    "candidates": candidates,
                    "num_candidates": len(candidates),
                    "matched_candidate": candidates[matched_index] if matched_index is not None else None,
                    "matched_candidate_index": matched_index,
                    "per_candidate": per_candidate,
                }
            )
            if matched_index is None and result.get("success") is not None:
                result["success"] = False
            return result
        finally:
            if backend is not None and hasattr(backend, "temperature"):
                backend.temperature = original_temperature
