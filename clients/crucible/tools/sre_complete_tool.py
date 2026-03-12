import logging
from pathlib import Path
from typing import Annotated

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.types import Command

logger = logging.getLogger(__name__)


def _make_complete_tool(tool_name: str, section_title: str, shared_file: Path, iteration: int):
    @tool(tool_name)
    async def complete(
        answer: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Submit your completed answer for this stage.

        Args:
            answer: Your final answer (hypothesis or mitigation strategy).
        """
        section = f"\n### Iteration {iteration} — {section_title}\n{answer}\n"
        try:
            with open(shared_file, "a") as f:
                f.write(section)
            logger.info(f"{section_title} written to {shared_file} (iteration {iteration})")
            result = f"{section_title} recorded. The judge will now evaluate. No further action needed."
        except Exception as e:
            logger.error(f"Failed to write {section_title.lower()}: {e}")
            result = f"Error recording {section_title.lower()}: {e}"

        return Command(
            update={"submitted": True, "messages": [ToolMessage(content=result, tool_call_id=tool_call_id)]}
        )

    return complete


def make_mark_hypothesis_complete(shared_file: Path, iteration: int):
    """Returns a tool that records the diagnosis hypothesis for the given iteration."""
    return _make_complete_tool("mark_hypothesis_complete", "Agent Hypothesis", shared_file, iteration)


def make_mark_mitigation_complete(shared_file: Path, iteration: int):
    """Returns a tool that records the mitigation strategy for the given iteration."""
    return _make_complete_tool("mark_mitigation_complete", "Agent Strategy", shared_file, iteration)
