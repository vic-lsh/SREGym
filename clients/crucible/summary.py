"""
Crucible long-term summarizer: reads the markdown shared session file,
strips <benchmark_result> blocks, and updates the long-term summary.

This enables cross-problem learning transfer — findings from one problem
are distilled into a long-term summary that is injected into the next
problem's agent prompt before the run starts.
"""

import logging
import re
from pathlib import Path

from clients.common.summarize import SUMMARY_FILENAME, get_llm_backend

logger = logging.getLogger(__name__)

_BENCHMARK_RESULT_RE = re.compile(
    r"<benchmark_result>.*?</benchmark_result>", re.DOTALL
)


def strip_benchmark_result(text: str) -> str:
    """Remove all <benchmark_result>...</benchmark_result> blocks from text."""
    return _BENCHMARK_RESULT_RE.sub("", text).strip()


class CrucibleLTSummarizer:
    """Maintains a long-term summary across Crucible sessions for cross-problem learning.

    Reads ``judged_session_state.md`` (the shared markdown file), strips
    ``<benchmark_result>`` blocks, then uses an LLM to:
      1. Summarize the current session (symptom, root cause, fix).
      2. Merge the session summary into the persistent long-term summary file.

    Args:
        shared_file: Path to ``judged_session_state.md`` written by the orchestrator.
        summary_dir: Directory where ``long_term_summary.txt`` is stored.
        model_id: Model identifier used for LLM summarization calls.
    """

    def __init__(self, shared_file: Path, summary_dir: Path, model_id: str):
        self.shared_file = shared_file
        self.summary_dir = Path(summary_dir)
        self.summary_dir.mkdir(parents=True, exist_ok=True)
        self.model_id = model_id
        self.summary_path = self.summary_dir / SUMMARY_FILENAME

    def _summarize_session(self, content: str) -> str:
        """Generate a concise summary of the current session from the shared markdown."""
        prompt = f"""You are analyzing an SRE session log written in Markdown. Summarize:
0) What is the symptom / problem?
1) What is the root cause (if identified)?
2) What fixes were applied or proposed (if any)?

DO NOT use external knowledge. Base your answer strictly on the text below.

Session log:
---
{content}
---
"""
        llm = get_llm_backend(self.model_id)
        result = llm.inference(messages=prompt)
        return result.content

    def _merge_into_long_term_summary(self, session_summary: str, prior_summary: str) -> str:
        """Merge the new session summary into the existing long-term summary."""
        prompt = f"""You are maintaining a long-term knowledge base of SRE incidents.

Current Long-Term Summary:
{prior_summary if prior_summary else "(Empty)"}

New Session Summary:
{session_summary}

Task: Update the Long-Term Summary.
- If the new session reveals a NEW problem, symptom, or solution, add it.
- If it is a recurrence of a previous problem, note the recurrence and increment the count.
- Output the updated Long-Term Summary text only, with no preamble.
"""
        llm = get_llm_backend(self.model_id)
        result = llm.inference(messages=prompt)
        return result.content

    def run(self) -> None:
        """Summarize the completed session and update the long-term summary file."""
        if not self.shared_file.exists():
            logger.warning(f"Shared file {self.shared_file} does not exist; skipping long-term summarization.")
            return

        raw = self.shared_file.read_text()
        content = strip_benchmark_result(raw)
        if not content:
            logger.warning("Shared file is empty after stripping benchmark results; skipping.")
            return

        logger.info("Generating session summary from shared markdown file...")
        try:
            session_summary = self._summarize_session(content)
        except Exception as e:
            logger.error(f"Failed to generate session summary: {e}")
            return

        logger.info(f"Session summary:\n{session_summary}")

        prior_summary = self.summary_path.read_text() if self.summary_path.exists() else ""
        logger.info("Merging into long-term summary...")
        try:
            updated = self._merge_into_long_term_summary(session_summary, prior_summary)
        except Exception as e:
            logger.error(f"Failed to merge into long-term summary: {e}")
            return

        self.summary_path.write_text(updated)
        logger.info(f"Long-term summary updated at {self.summary_path}")
