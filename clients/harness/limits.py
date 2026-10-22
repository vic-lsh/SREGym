# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The agent turn cap, set by the harness (main.py ``--max-turns``) for Claude-based clients."""

from collections.abc import Mapping

#: main.py exports ``--max-turns`` here; the Claude Code client passes it to ``claude --max-turns``.
MAX_TURNS_ENV = "AGENT_MAX_TURNS"


def max_turns_from_env(env: Mapping[str, str]) -> int | None:
    """The cap ``env`` names, or None when unset or empty.

    Raises:
        ValueError: The value is not a positive integer.
    """
    raw = env.get(MAX_TURNS_ENV, "").strip()
    if not raw:
        return None
    if not raw.isdigit() or int(raw) < 1:
        raise ValueError(f"{MAX_TURNS_ENV}={raw!r}: want a positive integer")
    return int(raw)


def max_turns_args(max_turns: int | None) -> list[str]:
    """``--max-turns N``, or nothing for no cap."""
    return ["--max-turns", str(max_turns)] if max_turns is not None else []
