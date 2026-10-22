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
"""clients/harness/usage.py on a recorded Claude Code stream-json log.

fixtures/claude_stream_json.jsonl is a real ``claude -p --output-format stream-json
--verbose`` run (Claude Code 2.1.306, claude-opus-5-5 on Vertex, two turns), with
text, thinking and tool payloads trimmed; every usage field is as recorded.
"""

import json
from pathlib import Path

import pytest

from clients.harness.usage import parse_stream_json, usage_from_logs

RECORDED = (Path(__file__).parent / "fixtures" / "claude_stream_json.jsonl").read_text().splitlines()
TASK_NOTIFICATION = (
    (Path(__file__).parent / "fixtures" / "claude_stream_json_task_notification.jsonl").read_text().splitlines()
)


def test_the_result_event_gives_the_totals_cache_writes_included() -> None:
    usage = parse_stream_json(RECORDED)
    assert (usage.input_tokens, usage.cache_creation_input_tokens, usage.cache_read_input_tokens) == (4, 34977, 0)
    assert usage.output_tokens == 481
    assert usage.total_input_tokens == 34981
    assert usage.total_cost_usd == pytest.approx(0.184521)
    assert (usage.num_turns, usage.duration_ms, usage.duration_api_ms) == (2, 7004, 6856)
    assert usage.result_events == 1 and usage.result_subtype == "success" and usage.complete


def test_a_run_killed_before_its_result_falls_back_to_assistant_messages() -> None:
    killed = [line for line in RECORDED if json.loads(line)["type"] != "result"]
    usage = parse_stream_json(killed)
    assert not usage.complete and usage.total_cost_usd is None and usage.result_events == 0
    # Two messages; the second spans two lines and is counted once.
    assert usage.num_turns == 2
    assert usage.cache_creation_input_tokens == 17331 + 17646
    assert usage.input_tokens == 4


def test_a_log_with_one_run_killed_is_incomplete_and_counts_that_run_from_its_messages() -> None:
    killed = [line for line in RECORDED if json.loads(line)["type"] != "result"]
    for log in (RECORDED + killed, killed + RECORDED):
        usage = parse_stream_json(log)
        assert not usage.complete and usage.total_cost_usd is None
        assert usage.result_events == 1
        assert usage.cache_creation_input_tokens == 34977 + 17331 + 17646
        assert usage.num_turns == 2 + 2


def test_result_events_of_separate_sessions_are_summed() -> None:
    other_session = [line.replace("30d67d45-761a-4991-8dd3-73d05e4e45b6", "another-session") for line in RECORDED]
    assert other_session != RECORDED
    usage = parse_stream_json(RECORDED + other_session)
    assert usage.result_events == 2 and usage.complete
    assert usage.cache_creation_input_tokens == 2 * 34977
    assert usage.total_cost_usd == pytest.approx(2 * 0.184521)
    assert usage.num_turns == 2 * 2


def test_a_session_woken_again_counts_its_last_result_once() -> None:
    # A real pilot run (fixtures/claude_stream_json_task_notification.jsonl, init and result
    # events only): a background task's notification woke the session for a second
    # init/result pair. modelUsage and total_cost_usd are cumulative, so summing them
    # reported $0.98 for a $0.50 run; usage, num_turns and durations are per result.
    usage = parse_stream_json(TASK_NOTIFICATION)
    assert usage.result_events == 2 and usage.complete
    assert usage.total_cost_usd == pytest.approx(0.5004208)
    assert (usage.input_tokens, usage.cache_creation_input_tokens) == (50, 42776)
    assert (usage.cache_read_input_tokens, usage.output_tokens) == (723304, 7084)
    assert (usage.num_turns, usage.duration_ms, usage.duration_api_ms) == (23 + 1, 376266 + 2117, 84725 + 86819)


def test_a_later_result_without_model_usage_adds_its_own_usage() -> None:
    session = {"type": "result", "session_id": "s", "total_cost_usd": 1.0}
    first = {**session, "modelUsage": {"m": {"inputTokens": 10, "outputTokens": 5}}}
    later = {**session, "total_cost_usd": 1.25, "usage": {"input_tokens": 2, "output_tokens": 1}}
    usage = parse_stream_json([json.dumps(first), json.dumps(later)])
    assert (usage.input_tokens, usage.output_tokens) == (12, 6)
    assert usage.total_cost_usd == pytest.approx(1.25)
    # Results without a session id are separate sessions, as before.
    anonymous = [json.dumps({**later, "session_id": None})] * 2
    assert parse_stream_json(anonymous).total_cost_usd == pytest.approx(2.5)


def test_model_usage_covers_every_model_and_usage_is_the_fallback() -> None:
    two_models = {
        "type": "result",
        "subtype": "error_max_turns",
        "num_turns": 40,
        "total_cost_usd": 1.5,
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "modelUsage": {
            "claude-opus-5-5": {"inputTokens": 10, "cacheCreationInputTokens": 100, "outputTokens": 5},
            "claude-haiku-5-5": {"inputTokens": 3, "cacheReadInputTokens": 7, "outputTokens": 2},
        },
    }
    usage = parse_stream_json([json.dumps(two_models)])
    assert (usage.input_tokens, usage.cache_creation_input_tokens, usage.cache_read_input_tokens) == (13, 100, 7)
    assert usage.output_tokens == 7 and usage.result_subtype == "error_max_turns"
    no_model_usage = {"type": "result", "usage": {"input_tokens": 2, "cache_read_input_tokens": 9, "output_tokens": 4}}
    usage = parse_stream_json([json.dumps(no_model_usage)])
    assert (usage.input_tokens, usage.cache_read_input_tokens, usage.output_tokens) == (2, 9, 4)


def test_lines_that_are_not_events_are_skipped() -> None:
    noise = ["", "not json", "[1, 2]", '{"type": "result", "usage": "oops", "num_turns": true}']
    usage = parse_stream_json(noise + RECORDED)
    assert usage.result_events == 2  # the malformed result counts, with zero tokens
    assert usage.output_tokens == 481


def test_usage_from_logs_reads_what_the_condition_left(tmp_path: Path) -> None:
    assert usage_from_logs(tmp_path) is None
    (tmp_path / "claude-code.txt").write_text("\n".join(RECORDED) + "\n")
    assert usage_from_logs(tmp_path).source == "stream-json result"


def test_result_fields_are_flat_and_prefixed() -> None:
    fields = parse_stream_json(RECORDED).result_fields()
    assert fields["usage.total_input_tokens"] == 34981
    assert fields["usage.total_cost_usd"] == pytest.approx(0.184521)
    assert all(not isinstance(v, dict | list) for v in fields.values())


def test_the_trajectory_cost_is_the_session_total_not_the_first_result(tmp_path: Path) -> None:
    from sregym.traces.convert import _claudecode_total_cost_usd

    (tmp_path / "claude-code.txt").write_text("\n".join(TASK_NOTIFICATION) + "\n")
    assert _claudecode_total_cost_usd(tmp_path) == pytest.approx(0.5004208)
    assert _claudecode_total_cost_usd(tmp_path / "missing") is None
