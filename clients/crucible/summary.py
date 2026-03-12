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
        prompt = f"""You are analyzing an SRE session log written in Markdown. Extract the following:
0) Application name (the specific service or component that had the incident).
1) Observed symptoms of that application (what was externally visible / what alerts fired).
2) Root cause (if identified).
3) Fixes or mitigations applied or proposed (if any).

DO NOT use external knowledge. Base your answer strictly on the text below.
Be concise and structured.

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

Task: Update the Long-Term Summary using the following structure:

## <Application Name>

### Symptom: <observed symptom or alert>
- **Root Causes:** <list of root causes seen for this symptom>
- **Mitigations:** <list of fixes or mitigations applied or proposed>

Rules:
- Group all incidents first by application name, then by observed symptom within that application.
- If the new session is for an application/symptom already in the summary, merge the new root causes and mitigations into the existing entry (avoid duplicates; note recurrences with a count if the same root cause appears again).
- If the new session reveals a new application or a new symptom for an existing application, add a new entry.
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
