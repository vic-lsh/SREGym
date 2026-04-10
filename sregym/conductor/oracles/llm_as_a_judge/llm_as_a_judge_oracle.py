"""LLM-as-a-Judge Oracle for evaluating agent solutions using LLM judgment.

Supports multi-round evaluation with majority voting for improved reliability.
Configure via JUDGE_NUM_ROUNDS (default 3) and JUDGE_VOTING_TEMPERATURE (default 0.7).

Multi-diagnosis support: ``evaluate`` accepts either a single string or a list
of candidate diagnoses. Each candidate is judged independently with the
existing voting prompt; the oracle short-circuits on the first candidate
that wins majority vote.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.judge import JudgmentResult, LLMJudge


class LLMAsAJudgeOracle(Oracle):
    """Oracle that uses an LLM judge to evaluate agent solutions against expected root causes."""

    def __init__(
        self,
        problem,
        expected: str,
        provider: Optional[str] = None,
        model_name: Optional[str] = None,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        num_rounds: Optional[int] = None,
        voting_temperature: Optional[float] = None,
    ):
        super().__init__(problem)
        self.expected = expected if expected else ""
        self.num_rounds = num_rounds if num_rounds is not None else int(os.environ.get("JUDGE_NUM_ROUNDS", "3"))
        self.voting_temperature = (
            voting_temperature if voting_temperature is not None
            else float(os.environ.get("JUDGE_VOTING_TEMPERATURE", "0.7"))
        )

        # Initialize the LLM judge
        self.judge = LLMJudge(
            provider=provider,
            model_name=model_name,
            url=url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    def _to_candidates(self, solution) -> list[str]:
        """Normalize ``solution`` into a list of candidate diagnosis strings.

        Accepts a bare string (wrapped into a singleton list) or an iterable of
        strings. Non-string elements are coerced via ``str``. Empty input is
        returned as an empty list, signalling the caller to fail fast.
        """
        if isinstance(solution, str):
            return [solution]
        if isinstance(solution, list):
            return [s if isinstance(s, str) else str(s) for s in solution]
        # Unexpected type — coerce to a singleton stringified list with a warning.
        print(f"⚠️  LLMAsAJudgeOracle: coercing {type(solution).__name__} solution to string")
        return [str(solution)]

    def _run_single_round(self, round_idx: int, solution: str) -> dict:
        """Execute a single judge round."""
        try:
            judgment, reasoning = self.judge.judge(solution=solution, expectation=self.expected)
            return {"round": round_idx, "judgment": judgment.value, "reasoning": reasoning}
        except Exception as e:
            return {"round": round_idx, "error": str(e)}

    def _evaluate_single_candidate(self, candidate: str) -> dict:
        """Run majority-vote judging for a single candidate diagnosis."""
        result: dict = {}
        try:
            round_details = []
            with ThreadPoolExecutor(max_workers=self.num_rounds) as executor:
                futures = {
                    executor.submit(self._run_single_round, i, candidate): i
                    for i in range(self.num_rounds)
                }
                for future in as_completed(futures):
                    round_details.append(future.result())

            round_details.sort(key=lambda x: x["round"])

            true_votes = sum(1 for r in round_details if r.get("judgment") == JudgmentResult.TRUE.value)
            false_votes = sum(1 for r in round_details if r.get("judgment") == JudgmentResult.FALSE.value)
            total_valid = true_votes + false_votes

            for r in round_details:
                status = r.get("judgment", f"ERROR: {r.get('error', 'unknown')}")
                print(f"   Round {r['round']}: {status}")

            if total_valid == 0:
                errors = "; ".join(r.get("error", "unknown") for r in round_details)
                print(f"❌ All {self.num_rounds} judge rounds failed")
                result["judgment"] = "Error"
                result["reasoning"] = f"All rounds failed: {errors}"
                result["success"] = False
                result["accuracy"] = 0.0
                result["error"] = errors
            else:
                is_correct = true_votes > false_votes
                final_judgment = JudgmentResult.TRUE if is_correct else JudgmentResult.FALSE
                winning_value = final_judgment.value
                reasoning = next(
                    (r["reasoning"] for r in round_details if r.get("judgment") == winning_value),
                    "",
                )

                acc = 100.0 if is_correct else 0.0
                if is_correct:
                    print(f"✅ Correct diagnosis (vote: {true_votes}/{total_valid})")
                else:
                    print(f"❌ Incorrect diagnosis (vote: {true_votes}/{total_valid})")
                    print(
                        f"   Expected: {self.expected[:100]}..."
                        if len(self.expected) > 100
                        else f"   Expected: {self.expected}"
                    )
                    print(f"   Got: {candidate[:100]}..." if len(candidate) > 100 else f"   Got: {candidate}")

                result["judgment"] = final_judgment.value
                result["reasoning"] = reasoning
                result["success"] = is_correct
                result["accuracy"] = acc
                result["vote_count"] = f"{true_votes}/{total_valid}"

            result["num_rounds"] = self.num_rounds
            result["round_details"] = json.dumps(round_details)

        except Exception as e:
            print(f"❌ Error during LLM judgment: {e}")
            result["judgment"] = "Error"
            result["reasoning"] = f"Error: {str(e)}"
            result["success"] = False
            result["accuracy"] = 0.0
            result["error"] = str(e)

        return result

    def evaluate(self, solution) -> dict:
        candidates = self._to_candidates(solution)
        print(
            f"== LLM-as-a-Judge Evaluation ({self.num_rounds} round(s), "
            f"{len(candidates)} candidate(s)) =="
        )

        # Empty candidate list → fail fast without touching the judge backend.
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

        # Force lazy backend init and override temperature for voting diversity.
        original_temperature = None
        if self.num_rounds > 1:
            backend = self.judge.backend  # triggers lazy init
            original_temperature = backend.temperature
            backend.temperature = self.voting_temperature
            print(f"   Voting temperature: {self.voting_temperature} (was {original_temperature})")

        try:
            per_candidate: list[dict] = []
            matched_candidate: str | None = None
            matched_candidate_index: int | None = None
            winning_result: dict | None = None

            for idx, candidate in enumerate(candidates):
                if len(candidates) > 1:
                    print(f"-- Candidate {idx + 1}/{len(candidates)} --")
                cand_result = self._evaluate_single_candidate(candidate)
                cand_result["candidate"] = candidate
                per_candidate.append(cand_result)

                if cand_result.get("success"):
                    matched_candidate = candidate
                    matched_candidate_index = idx
                    winning_result = cand_result
                    # Short-circuit: any matching candidate is sufficient.
                    break

            if winning_result is None:
                # No candidate passed — surface the last attempt's vote details.
                final = per_candidate[-1]
                top_level = {
                    "judgment": final.get("judgment", JudgmentResult.FALSE.value),
                    "reasoning": final.get("reasoning", ""),
                    "success": False,
                    "accuracy": 0.0,
                    "num_rounds": self.num_rounds,
                    "round_details": final.get("round_details", "[]"),
                }
                if "vote_count" in final:
                    top_level["vote_count"] = final["vote_count"]
                if "error" in final:
                    top_level["error"] = final["error"]
            else:
                top_level = {
                    "judgment": winning_result["judgment"],
                    "reasoning": winning_result["reasoning"],
                    "success": True,
                    "accuracy": winning_result.get("accuracy", 100.0),
                    "num_rounds": self.num_rounds,
                    "round_details": winning_result.get("round_details", "[]"),
                }
                if "vote_count" in winning_result:
                    top_level["vote_count"] = winning_result["vote_count"]

            top_level["candidates"] = candidates
            top_level["num_candidates"] = len(candidates)
            top_level["matched_candidate"] = matched_candidate
            top_level["matched_candidate_index"] = matched_candidate_index
            top_level["per_candidate"] = per_candidate
            return top_level

        finally:
            if original_temperature is not None:
                self.judge.backend.temperature = original_temperature
