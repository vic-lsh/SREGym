import asyncio
import logging
import shlex
import uuid
from pathlib import Path
from typing import Annotated

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.types import Command

logger = logging.getLogger(__name__)

MUTATING_KUBECTL_VERBS = frozenset(
    {"apply", "delete", "patch", "edit", "scale", "set", "replace", "create", "rollout"}
)
MAX_OUTPUT_CHARS = 4000
BASH_TIMEOUT = 60


async def _run_bash(cmd: str) -> str:
    """Run a bash command and return combined output (stdout + stderr on nonzero exit)."""
    try:
        try:
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_shell(
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                ),
                timeout=BASH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return f"Error: Command timed out after {BASH_TIMEOUT} seconds."

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=BASH_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return f"Error: Command timed out after {BASH_TIMEOUT} seconds."

        output = stdout.decode("utf-8", errors="replace")
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace")
            if err:
                output += f"\nSTDERR: {err}"
    except Exception as e:
        return f"Error executing command: {e}"

    if len(output) > MAX_OUTPUT_CHARS:
        tmp_path = f"/tmp/bash_out_{uuid.uuid4().hex}.txt"
        Path(tmp_path).write_text(output)
        return (
            f"Output truncated ({len(output)} chars). Written to {tmp_path}. "
            f"Use read_file to inspect it (e.g. read_file(path='{tmp_path}', start_line=0, end_line=100))."
        )
    return output or "(no output)"


def _check_mutating_kubectl(cmd: str) -> str | None:
    """Return an error string if cmd contains a mutating kubectl verb, else None."""
    try:
        tokens = shlex.split(cmd)
    except ValueError:
        tokens = cmd.split()

    if "kubectl" not in tokens:
        return None

    kubectl_idx = tokens.index("kubectl")
    for token in tokens[kubectl_idx + 1 :]:
        if not token.startswith("-"):
            if token.lower() in MUTATING_KUBECTL_VERBS:
                return (
                    f"Error: As a judge, you are not allowed to run mutating kubectl "
                    f"commands (verb='{token}'). Your role is read-only evaluation only."
                )
            break
    return None


@tool
async def exec_bash(cmd: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> Command:
    """Execute a bash command and return its output.

    If output exceeds 4000 chars it is written to a temp file — use read_file to view it.

    Args:
        cmd: The bash command to execute.
    """
    content = await _run_bash(cmd)
    return Command(update={"messages": [ToolMessage(content=content, tool_call_id=tool_call_id)]})


@tool
async def exec_bash_readonly(cmd: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> Command:
    """Execute a read-only bash command. Mutating kubectl verbs (apply, delete, patch, etc.) are blocked.

    Args:
        cmd: The bash command to execute (read-only; kubectl mutations are not allowed).
    """
    err = _check_mutating_kubectl(cmd)
    if err:
        return Command(update={"messages": [ToolMessage(content=err, tool_call_id=tool_call_id)]})
    content = await _run_bash(cmd)
    return Command(update={"messages": [ToolMessage(content=content, tool_call_id=tool_call_id)]})
