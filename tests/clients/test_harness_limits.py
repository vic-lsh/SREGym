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
"""--max-turns: validated, and passed to claude and to parallel workers (main.py defaults it to 300)."""

import pytest

from clients.harness.limits import MAX_TURNS_ENV, max_turns_args, max_turns_from_env


def test_no_cap_unless_set() -> None:
    assert max_turns_from_env({}) is None
    assert max_turns_from_env({MAX_TURNS_ENV: " "}) is None
    assert max_turns_args(None) == []


def test_a_cap_becomes_the_claude_flag() -> None:
    assert max_turns_from_env({MAX_TURNS_ENV: "40"}) == 40
    assert max_turns_args(40) == ["--max-turns", "40"]


@pytest.mark.parametrize("raw", ["0", "-3", "ten", "1.5"])
def test_a_bad_cap_is_refused(raw: str) -> None:
    with pytest.raises(ValueError):
        max_turns_from_env({MAX_TURNS_ENV: raw})


def test_parallel_workers_inherit_the_cap() -> None:
    from sregym.parallel_runner import RunTask, _args_for_tests, _child_command

    args = _args_for_tests(problem="target_port")
    args.agent, args.model, args.agent_timeout, args.judge_model = "claudecode", "sonnet", 90, "judge"
    args.reasoning_effort = None
    args.noise = args.force_build = args.use_external_harness = False
    assert "--max-turns" not in _child_command(args, RunTask(0, "target_port"))
    args.max_turns = 40
    command = _child_command(args, RunTask(0, "target_port"))
    assert command[command.index("--max-turns") + 1] == "40"
    args.max_turns = 0  # no cap must reach the child, which would otherwise default to 300
    command = _child_command(args, RunTask(0, "target_port"))
    assert command[command.index("--max-turns") + 1] == "0"
