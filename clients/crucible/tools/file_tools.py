import logging
from pathlib import Path
from typing import Annotated

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.types import Command

logger = logging.getLogger(__name__)

READ_FILE_MAX_CHARS = 2000


@tool
def read_file(
    path: str,
    start_line: int,
    end_line: int,
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Read lines [start_line, end_line) from a file in cat -n notation.

    Output is truncated at READ_FILE_MAX_CHARS characters. Call again with
    adjusted start_line/end_line to read further.

    Args:
        path: Absolute path to the file.
        start_line: First line to read (0-indexed, inclusive).
        end_line: Last line to read (0-indexed, exclusive). Use -1 for end of file.
    """
    try:
        lines = Path(path).read_text().splitlines()
        start = max(0, start_line)
        end = len(lines) if end_line == -1 else min(end_line, len(lines))
        numbered = "\n".join(f"{start + i + 1:6}\t{line}" for i, line in enumerate(lines[start:end]))
        content = numbered or "(empty range)"
        if len(content) > READ_FILE_MAX_CHARS:
            content = content[:READ_FILE_MAX_CHARS] + f"\n... (truncated at {READ_FILE_MAX_CHARS} chars — call read_file again with a higher start_line to continue)"
    except FileNotFoundError:
        content = f"Error: File not found: {path}"
    except Exception as e:
        content = f"Error reading file: {e}"

    return Command(update={"messages": [ToolMessage(content=content, tool_call_id=tool_call_id)]})


@tool
def write_file(
    path: str,
    content: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Create or overwrite a file with the given content.

    Args:
        path: Absolute path to write.
        content: Content to write to the file.
    """
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        result = f"File written: {path} ({len(content)} chars)"
    except Exception as e:
        result = f"Error writing file: {e}"

    return Command(update={"messages": [ToolMessage(content=result, tool_call_id=tool_call_id)]})


@tool
def str_replace_file(
    path: str,
    old_str: str,
    new_str: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Replace the first occurrence of old_str with new_str in a file.

    Args:
        path: Absolute path to the file.
        old_str: The exact string to find (must appear at least once).
        new_str: The replacement string.
    """
    try:
        p = Path(path)
        text = p.read_text()
        if old_str not in text:
            result = f"Error: old_str not found in {path}"
        else:
            p.write_text(text.replace(old_str, new_str, 1))
            result = f"Replaced first occurrence in {path}"
    except FileNotFoundError:
        result = f"Error: File not found: {path}"
    except Exception as e:
        result = f"Error modifying file: {e}"

    return Command(update={"messages": [ToolMessage(content=result, tool_call_id=tool_call_id)]})
