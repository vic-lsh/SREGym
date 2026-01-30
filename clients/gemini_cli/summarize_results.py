"""
Summarize Gemini CLI execution results.
Separated from the agent process to ensure it runs even if the agent is killed.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Add SREGym root to path to import backend
sregym_root = Path(__file__).resolve().parents[2]
if str(sregym_root) not in sys.path:
    sys.path.insert(0, str(sregym_root))

from llm_backend.get_llm_backend import LiteLLMBackend
from logger import init_logger

init_logger()
logger = logging.getLogger("all.gemini_cli.summarize")


class ResultSummarizer:
    def __init__(self, logs_dir: Path, model_name: str):
        self.logs_dir = logs_dir
        self.model_name = model_name.removeprefix("vertex-ai-")
        
        self.output_path = self.logs_dir / "gemini-cli.txt"
        self.instruction_path = self.logs_dir / "instruction.txt"
        self.summary_path = self.logs_dir / "long_term_summary.txt"

    def _get_response_text(self) -> str:
        """Extract response text from Gemini CLI output file."""
        text = ""
        if not self.output_path.exists():
            logger.warning(f"Output file {self.output_path} does not exist.")
            return text

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
        api_key = os.environ.get("GOOGLE_API_KEY", "") or os.environ.get("GEMINI_API_KEY", "")
        model_name = self.model_name
        
        # Determine appropriate model prefix based on available credentials
        provider = "litellm"
        location = None

        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            # Use Vertex AI if service account credentials are provided
            provider = "vertexai"
            if not model_name.startswith("vertex_ai/"):
                model_name = f"vertex_ai/{model_name}"
            # Check for location in env vars
            location = os.environ.get("VERTEX_LOCATION") or os.environ.get("GOOGLE_CLOUD_LOCATION") or os.environ.get("LOCATION")

        elif not model_name.startswith("gemini/") and "gemini" in model_name:
            # Fallback to AI Studio (requires API Key)
             model_name = f"gemini/{model_name}"

        try:
            llm = LiteLLMBackend(
                provider=provider,
                model_name=model_name,
                api_key=api_key,
                temperature=0.0,
                location=location
            )
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
        api_key = os.environ.get("GOOGLE_API_KEY", "") or os.environ.get("GEMINI_API_KEY", "")
        model_name = self.model_name
        
        # Determine appropriate model prefix based on available credentials
        provider = "litellm"
        location = None

        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            # Use Vertex AI if service account credentials are provided
            provider = "vertexai"
            if not model_name.startswith("vertex_ai/"):
                model_name = f"vertex_ai/{model_name}"
            # Check for location in env vars
            location = os.environ.get("VERTEX_LOCATION") or os.environ.get("GOOGLE_CLOUD_LOCATION") or os.environ.get("LOCATION")

        elif not model_name.startswith("gemini/") and "gemini" in model_name:
            # Fallback to AI Studio (requires API Key)
             model_name = f"gemini/{model_name}"

        try:
            llm = LiteLLMBackend(
                provider=provider,
                model_name=model_name,
                api_key=api_key,
                temperature=0.0,
                location=location
            )
            result = llm.inference(messages=prompt)
            return result.content
        except Exception as e:
            logger.error(f"Failed to merge summaries: {e}")
            raise

    def run(self):
        logger.info(f"Summarizing run in {self.logs_dir}")

        if not self.instruction_path.exists():
            logger.error(f"Instruction file not found at {self.instruction_path}. Cannot summarize.")
            sys.exit(1)

        instruction = self.instruction_path.read_text()
        response = self._get_response_text()

        if not response:
            logger.warning("Response text is empty. Agent might have produced no output.")
            # We still try to summarize even if response is empty (maybe to note failure), 
            # but usually it's better to note that no response was recorded.
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
            # Do not raise, just log error so we don't crash the harness if this optional step fails
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Summarize Gemini CLI results")
    parser.add_argument("--logs-dir", type=str, required=True, help="Path to logs directory")
    parser.add_argument("--model", type=str, required=True, help="Model name used")
    args = parser.parse_args()

    summarizer = ResultSummarizer(Path(args.logs_dir), args.model)
    summarizer.run()

if __name__ == "__main__":
    main()
