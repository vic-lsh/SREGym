import ast
import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Annotated

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.types import Command
from mcp import ClientSession
from mcp.client.sse import sse_client

from clients.stratus.configs.langgraph_tool_configs import LanggraphToolConfig

logger = logging.getLogger(__name__)

_tool_config = LanggraphToolConfig()


async def _submit_to_benchmark(submission_ans: str) -> tuple[bool, str]:
    """Submit answer via MCP SSE. Returns (success, message)."""
    try:
        async with AsyncExitStack() as stack:
            http_transport = await stack.enter_async_context(
                sse_client(url=_tool_config.submit_mcp_url)
            )
            session = await stack.enter_async_context(ClientSession(*http_transport))
            await session.initialize()
            result = await session.call_tool("submit", arguments={"ans": submission_ans})
            result = ast.literal_eval(result.content[0].text)
            if result.get("status") == "200":
                return True, "Submission accepted by benchmark."
            return False, f"Benchmark rejected submission: {result}"
    except Exception as e:
        return False, f"Submission error: {e}"


def make_approve_and_submit(shared_file: Path, iteration: int, stage: str):
    """Factory: returns an approve_and_submit tool bound to the given shared file, iteration, and stage."""

    @tool(name="approve_and_submit")
    async def approve_and_submit(
        submission_ans: str,
        reasoning: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Approve the agent's work and submit the answer to the benchmark.

        Call this ONLY when you are confident the agent's work is correct.

        Args:
            submission_ans: The answer string to submit to the benchmark.
            reasoning: Your detailed reasoning for approving this answer.
        """
        section = (
            f"\n### Iteration {iteration} — Judge Verdict ({stage})\n"
            f"- Status: APPROVED\n"
            f"- Reasoning: {reasoning}\n"
        )
        try:
            with open(shared_file, "a") as f:
                f.write(section)
        except Exception as e:
            logger.error(f"Failed to write approval to shared file: {e}")

        success, msg = await _submit_to_benchmark(submission_ans)
        if success:
            content = f"APPROVED. {msg}"
            logger.info(f"Judge approved and submitted (iteration {iteration}, stage {stage})")
        else:
            logger.warning(f"Approval recorded but submission failed: {msg}")
            content = f"APPROVED and recorded, but submission encountered an error: {msg}"

        return Command(
            update={
                "submitted": True,
                "verdict": "APPROVED",
                "messages": [ToolMessage(content=content, tool_call_id=tool_call_id)],
            }
        )

    return approve_and_submit


def make_reject_with_feedback(shared_file: Path, iteration: int, stage: str):
    """Factory: returns a reject_with_feedback tool bound to the given shared file, iteration, and stage."""

    @tool(name="reject_with_feedback")
    async def reject_with_feedback(
        feedback: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Reject the agent's work and provide actionable feedback for the next iteration.

        Call this when the agent's work is incorrect or incomplete.

        Args:
            feedback: Specific, actionable feedback explaining what is wrong and what
                      the agent should investigate or do differently on the next attempt.
        """
        section = (
            f"\n### Iteration {iteration} — Judge Verdict ({stage})\n"
            f"- Status: REJECTED\n"
            f"- Feedback: {feedback}\n"
        )
        try:
            with open(shared_file, "a") as f:
                f.write(section)
            logger.info(f"Judge rejected with feedback (iteration {iteration}, stage {stage})")
        except Exception as e:
            logger.error(f"Failed to write rejection to shared file: {e}")

        return Command(
            update={
                "submitted": True,
                "verdict": "REJECTED",
                "messages": [
                    ToolMessage(
                        content="REJECTED. Feedback recorded in the shared session file. The agent will retry.",
                        tool_call_id=tool_call_id,
                    )
                ],
            }
        )

    return reject_with_feedback
