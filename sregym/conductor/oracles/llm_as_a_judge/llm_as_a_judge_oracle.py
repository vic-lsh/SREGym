"""LLM-as-a-Judge Oracle for evaluating agent solutions using LLM judgment.

Supports multi-round evaluation with majority voting for improved reliability.
Configure via JUDGE_NUM_ROUNDS (default 3) and JUDGE_VOTING_TEMPERATURE (default 0.7).
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

    def _run_single_round(self, round_idx: int, solution: str) -> dict:
        """Execute a single judge round."""
        try:
            judgment, reasoning = self.judge.judge(solution=solution, expectation=self.expected)
            return {"round": round_idx, "judgment": judgment.value, "reasoning": reasoning}
        except Exception as e:
            return {"round": round_idx, "error": str(e)}

    def evaluate(self, solution) -> dict:
        print(f"== LLM-as-a-Judge Evaluation ({self.num_rounds} round(s)) ==")
        results = {}

        if not isinstance(solution, str):
            solution = str(solution)

        # Force lazy backend init and override temperature for voting diversity
        original_temperature = None
        if self.num_rounds > 1:
            backend = self.judge.backend  # triggers lazy init
            original_temperature = backend.temperature
            backend.temperature = self.voting_temperature
            print(f"   Voting temperature: {self.voting_temperature} (was {original_temperature})")

        try:
            round_details = []
            with ThreadPoolExecutor(max_workers=self.num_rounds) as executor:
                futures = {
                    executor.submit(self._run_single_round, i, solution): i
                    for i in range(self.num_rounds)
                }
                for future in as_completed(futures):
                    round_details.append(future.result())

            round_details.sort(key=lambda x: x["round"])

            # Tally votes from successful rounds
            true_votes = sum(1 for r in round_details if r.get("judgment") == JudgmentResult.TRUE.value)
            false_votes = sum(1 for r in round_details if r.get("judgment") == JudgmentResult.FALSE.value)
            total_valid = true_votes + false_votes

            for r in round_details:
                status = r.get("judgment", f"ERROR: {r.get('error', 'unknown')}")
                print(f"   Round {r['round']}: {status}")

            if total_valid == 0:
                # All rounds errored
                errors = "; ".join(r.get("error", "unknown") for r in round_details)
                print(f"❌ All {self.num_rounds} judge rounds failed")
                results["judgment"] = "Error"
                results["reasoning"] = f"All rounds failed: {errors}"
                results["success"] = False
                results["accuracy"] = 0.0
                results["error"] = errors
            else:
                # Majority vote (ties go to FALSE / conservative)
                is_correct = true_votes > false_votes
                final_judgment = JudgmentResult.TRUE if is_correct else JudgmentResult.FALSE

                # Pick reasoning from a majority-side round
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
                    print(f"   Got: {solution[:100]}..." if len(solution) > 100 else f"   Got: {solution}")

                results["judgment"] = final_judgment.value
                results["reasoning"] = reasoning
                results["success"] = is_correct
                results["accuracy"] = acc
                results["vote_count"] = f"{true_votes}/{total_valid}"

            results["num_rounds"] = self.num_rounds
            results["round_details"] = json.dumps(round_details)

        except Exception as e:
            print(f"❌ Error during LLM judgment: {e}")
            results["judgment"] = "Error"
            results["reasoning"] = f"Error: {str(e)}"
            results["success"] = False
            results["accuracy"] = 0.0
            results["error"] = str(e)

        finally:
            if original_temperature is not None:
                self.judge.backend.temperature = original_temperature

        return results
