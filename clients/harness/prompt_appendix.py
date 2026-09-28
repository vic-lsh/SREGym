"""Optional, experiment-supplied text appended to a native agent's instruction."""

from __future__ import annotations

import os

#: Environment variable holding the appendix. Unset or blank leaves the
#: instruction byte-for-byte unchanged, so stock baselines are unaffected.
PROMPT_APPENDIX_ENV = "SREGYM_AGENT_PROMPT_APPENDIX"


def append_prompt_appendix(instruction: str) -> str:
    """Append the supervisor-provided appendix, if any, after a blank line."""
    appendix = os.environ.get(PROMPT_APPENDIX_ENV, "").strip()
    if not appendix:
        return instruction
    return f"{instruction}\n\n{appendix}"
