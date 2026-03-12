import logging
from pathlib import Path
from typing import Annotated

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.types import Command

logger = logging.getLogger(__name__)


def make_mark_hypothesis_complete(shared_file: Path, iteration: int):
    """Returns a tool that records the diagnosis hypothesis for the given iteration."""

    @tool
    async def mark_hypothesis_complete(
        diagnosis: str,
        justification: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Submit your completed diagnosis hypothesis for this stage.

        Args:
            diagnosis: Your final diagnosis — the specific root cause of the issue.
            justification: Detailed justification explaining the evidence that supports
                           your diagnosis.
        """
        section = (
            f"\n### Iteration {iteration} — Agent Hypothesis\n"
            f"**Diagnosis**: {diagnosis}\n"
            f"**Justification**: {justification}\n"
        )
        try:
            with open(shared_file, "a") as f:
                f.write(section)
            logger.info(f"Agent Hypothesis written to {shared_file} (iteration {iteration})")
            result = "Agent Hypothesis recorded. The judge will now evaluate. No further action needed."
        except Exception as e:
            logger.error(f"Failed to write agent hypothesis: {e}")
            result = f"Error recording agent hypothesis: {e}"

        return Command(
            update={"submitted": True, "messages": [ToolMessage(content=result, tool_call_id=tool_call_id)]}
        )

    return mark_hypothesis_complete


def make_mark_mitigation_complete(shared_file: Path, iteration: int):
    """Returns a tool that records the mitigation strategy for the given iteration."""

    @tool
    async def mark_mitigation_complete(
        mitigation: str,
        justification: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Submit your completed mitigation for this stage.

        Args:
            mitigation: A concise description of the fix you applied.
            justification: Detailed justification explaining why this fix addresses the
                           root cause and evidence that it has taken effect.
        """
        section = (
            f"\n### Iteration {iteration} — Agent Strategy\n"
            f"**Mitigation**: {mitigation}\n"
            f"**Justification**: {justification}\n"
        )
        try:
            with open(shared_file, "a") as f:
                f.write(section)
            logger.info(f"Agent Strategy written to {shared_file} (iteration {iteration})")
            result = "Agent Strategy recorded. The judge will now evaluate. No further action needed."
        except Exception as e:
            logger.error(f"Failed to write agent strategy: {e}")
            result = f"Error recording agent strategy: {e}"

        return Command(
            update={"submitted": True, "messages": [ToolMessage(content=result, tool_call_id=tool_call_id)]}
        )

    return mark_mitigation_complete
