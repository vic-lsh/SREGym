"""
Shared summarization of agent execution results.
Works for any agent (Gemini CLI, Claude Code, etc.) by parameterizing
the output filename and using configs.yaml-based LLM construction.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

# Add SREGym root to path to import backend
sregym_root = Path(__file__).resolve().parents[2]
if str(sregym_root) not in sys.path:
    sys.path.insert(0, str(sregym_root))

from llm_backend.get_llm_backend import LiteLLMBackend
from llm_backend.init_backend import get_llm_backend_for_model
from logger import init_logger

init_logger()
logger = logging.getLogger("all.common.summarize")


class ResultSummarizer:
    def __init__(
        self,
        logs_dir: Path,
        model_id: str,
        output_filename: str,
        summary_dir: Optional[Path] = None,
    ):
        self.logs_dir = logs_dir
        self.model_id = model_id
        self.output_filename = output_filename
        self.summary_dir = summary_dir if summary_dir else logs_dir

        self.output_path = self.logs_dir / self.output_filename
        self.summary_path = self.summary_dir / "long_term_summary.txt"

    def _get_llm(self) -> LiteLLMBackend:
        """Build an LLM backend using configs.yaml, with litellm fallback."""
        try:
            return get_llm_backend_for_model(self.model_id)
        except ValueError:
            logger.warning(
                f"Model {self.model_id} not in configs.yaml, falling back to direct litellm construction"
            )
            return LiteLLMBackend(
                provider="litellm", model_name=self.model_id, temperature=0.0
            )

    def _get_instruction_text(self) -> str:
        """Extract instruction from instruction.txt, or fall back to the output file."""
        instruction_file = self.logs_dir / "instruction.txt"
        if instruction_file.exists():
            try:
                return instruction_file.read_text().strip()
            except Exception as e:
                logger.warning(f"Failed to read instruction.txt: {e}")

        if not self.output_path.exists():
            return ""

        try:
            with open(self.output_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        if event.get("type") == "message" and event.get("role") == "user":
                            return event.get("content", "")
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error reading instruction from output file: {e}")

        return ""

    def _get_response_text(self) -> str:
        """Read entire output file as response text."""
        if not self.output_path.exists():
            logger.warning(f"Output file {self.output_path} does not exist.")
            return ""

        logger.info(f"Reading output file {self.output_path}")
        with open(self.output_path, "r") as f:
            return f.read()

    def _summarize_trajectory(self, instruction: str, response: str) -> str:
        """Generate a summary of the current iteration using an LLM."""
        prompt = f"""
Analyze the following interaction trajectory of an SRE agent. Summarize:
0) What is the symptom?
1) What is the root cause (if diagnosed)?
2) What are the fixes (if any)?

DO NOT use external knowledge. Strictly use the provided text.

Trajectory:
---
Instruction:
{instruction}

Response:
{response}
---
"""
        llm = self._get_llm()
        try:
            result = llm.inference(messages=prompt)
            return result.content
        except Exception as e:
            logger.error(f"Failed to generate iteration summary: {e}")
            raise

    def _merge_summaries(self, iteration_summary: str) -> str:
        """Merge iteration summary with long-term summary."""
        current_summary = ""
        if self.summary_path.exists():
            with open(self.summary_path, "r") as f:
                current_summary = f.read()

        prompt = f"""
You are maintaining a long-term summary of SRE incidents.

Current Long-Term Summary:
{current_summary if current_summary else "(Empty)"}

New Iteration Summary:
{iteration_summary}

Task: Update the Long-Term Summary.
- If the new iteration reveals a NEW problem, symptom, or solution, add it to the summary.
- If it is a reoccurrence of a previous problem, increment a count or note the reoccurrence in the summary.
- Output the updated Long-Term Summary text only.
"""
        llm = self._get_llm()
        try:
            result = llm.inference(messages=prompt)
            return result.content
        except Exception as e:
            logger.error(f"Failed to merge summaries: {e}")
            raise

    def run(self):
        logger.info(f"Summarizing run in {self.logs_dir}")

        instruction = self._get_instruction_text()
        if not instruction:
            logger.error("Instruction not found in output file. Cannot summarize.")
            sys.exit(1)
        response = self._get_response_text()

        if not response:
            logger.warning("Response text is empty. Agent might have produced no output.")
            response = "(No output from agent)"

        try:
            logger.info("Generating iteration summary...")
            iter_summary = self._summarize_trajectory(instruction, response)
            logger.info(f"Iteration Summary: {iter_summary}")

            logger.info("Merging with long-term summary...")
            updated_long_term = self._merge_summaries(iter_summary)
            logger.info(f"Updated Long-Term Summary: {updated_long_term}")

            with open(self.summary_path, "w") as f:
                f.write(updated_long_term)
            logger.info(f"Updated long-term summary saved to {self.summary_path}")

        except Exception as e:
            logger.error(f"Summarization failed: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Summarize agent execution results")
    parser.add_argument("--logs-dir", type=str, required=True, help="Path to logs directory")
    parser.add_argument("--model", type=str, required=True, help="Model ID for the summarization LLM")
    parser.add_argument(
        "--output-filename",
        type=str,
        default="gemini-cli.txt",
        help="Name of the agent output file (default: gemini-cli.txt)",
    )
    parser.add_argument("--summary-dir", type=str, required=False, help="Path to summary directory")
    args = parser.parse_args()

    summary_dir = Path(args.summary_dir) if args.summary_dir else None
    summarizer = ResultSummarizer(
        Path(args.logs_dir), args.model, args.output_filename, summary_dir=summary_dir
    )
    summarizer.run()


if __name__ == "__main__":
    main()
