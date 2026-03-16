import ast
import json
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


async def submit_to_benchmark(submission_ans: str, stage: str) -> tuple[bool, str, dict | None]:
    """Submit answer via MCP SSE. Returns (success, message, oracle_result_dict)."""
    try:
        async with AsyncExitStack() as stack:
            http_transport = await stack.enter_async_context(sse_client(url=_tool_config.submit_mcp_url))
            session = await stack.enter_async_context(ClientSession(*http_transport))
            await session.initialize()
            result = await session.call_tool("submit", arguments={"ans": submission_ans})
            result = ast.literal_eval(result.content[0].text)
            if result.get("status") != "200":
                return False, f"Benchmark rejected submission: {result}", None
            # HTTP 200 means the request was received; check the actual evaluation result
            try:
                eval_result = json.loads(result.get("text", "{}"))
                stage_key = stage.capitalize()
                if stage_key in eval_result:
                    stage_result = eval_result[stage_key]
                    if not stage_result.get("success", False):
                        reasoning = stage_result.get("reasoning", "")
                        return (
                            False,
                            (f"Benchmark evaluated submission as incorrect. Reasoning: {reasoning}"),
                            stage_result,
                        )
                    return True, "Submission accepted by benchmark.", stage_result
            except (json.JSONDecodeError, AttributeError):
                pass
            return True, "Submission accepted by benchmark.", None
    except Exception as e:
        return False, f"Submission error: {e}", None


def make_submit_verdict(shared_file: Path, iteration: int, stage: str):
    """Factory: returns a submit_verdict tool bound to the given shared file, iteration, and stage."""

    @tool
    async def submit_verdict(
        verdict: bool,
        reasoning: str,
        submission_ans: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> Command:
        """Submit your evaluation verdict.

        Args:
            verdict: True to approve the agent's work, False to reject it.
            reasoning: Detailed reasoning for your verdict, including evidence from
                       cluster inspection.
            submission_ans: The answer string to submit to the benchmark. Use the
                            agent's hypothesis for diagnosis, empty string for mitigation.
        """
        status = "APPROVED" if verdict else "REJECTED"
        section = (
            f"\n### Iteration {iteration} — Judge Verdict ({stage})\n- Status: {status}\n- Reasoning: {reasoning}\n"
        )
        try:
            with open(shared_file, "a") as f:
                f.write(section)
        except Exception as e:
            logger.error(f"Failed to write verdict to shared file: {e}")

        if verdict:
            success, msg, oracle_result = await submit_to_benchmark(submission_ans, stage)
            if success:
                content = f"APPROVED. {msg}"
                logger.info(f"Judge approved and submitted (iteration {iteration}, stage {stage})")
            else:
                logger.warning(f"Judge approved but benchmark evaluated as incorrect: {msg}")
                content = f"APPROVED and submitted, but benchmark evaluated as incorrect: {msg}"

            try:
                with open(shared_file, "a") as f:
                    f.write(
                        f"<benchmark_result>\nThis is an oracle response that supersedes the previous findings"
                        f" from the agent and the judge.\n{json.dumps(oracle_result, indent=2) if oracle_result is not None else msg}\n</benchmark_result>\n"
                    )
            except Exception as e:
                logger.error(f"Failed to write benchmark result to shared file: {e}")
        else:
            content = "REJECTED. Feedback recorded in the shared session file. The agent will retry."
            logger.info(f"Judge rejected with feedback (iteration {iteration}, stage {stage})")

        return Command(
            update={
                "submitted": True,
                "verdict": status,
                "messages": [ToolMessage(content=content, tool_call_id=tool_call_id)],
            }
        )

    return submit_verdict
