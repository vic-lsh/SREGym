"""Optional cross-run operational-memory injection for native agents."""

from __future__ import annotations

import os
from pathlib import Path


def inject_operational_memory(instruction: str) -> str:
    """Append bounded experiment memory when the supervisor provides it."""
    memory_file = os.environ.get("SREGYM_SUMMARY_FILE")
    if not memory_file:
        return instruction
    path = Path(memory_file)
    if not path.is_file():
        return instruction
    memory = path.read_text(encoding="utf-8").strip()
    if not memory:
        return instruction
    return (
        f"{instruction}\n\n"
        "OPERATIONAL MEMORY FROM EARLIER BENCHMARK RUNS:\n"
        f"{memory[-50000:]}\n\n"
        "Use this as prior evidence, but verify it against the current cluster before acting."
    )
