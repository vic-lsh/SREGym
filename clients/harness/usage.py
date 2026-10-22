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
"""Token, cost and turn accounting for one agent run, in one schema for every condition.

Claude Code's ``--output-format stream-json`` ends each ``claude -p`` with a ``result``
event carrying the session's totals: ``usage`` (uncached input, cache writes, cache
reads, output), ``modelUsage`` (the same per model, including any subagent model, with
``costUSD``), ``total_cost_usd``, ``num_turns``, ``duration_ms`` and ``duration_api_ms``.
A ``claude -p`` emits a further ``system``/``init`` and ``result`` pair, under the same
``session_id``, each time a backgrounded task's notification wakes it for another turn.
``modelUsage`` and ``total_cost_usd`` are cumulative over the session; ``usage``,
``num_turns`` and the durations cover only the turns since the previous result.
``parse_stream_json`` therefore takes each session's last ``modelUsage`` and cost (plus
the ``usage`` of any later result that lacks ``modelUsage``), sums the per-result fields,
and sums across sessions. A result without a ``session_id`` is its own session.

A run killed before its result event (the harness timeout) has none. Its counts
fall back to the ``usage`` of each assistant message it streamed, taken once per
message id; the stream reports a message's usage when it starts, so its output
count is low. A log with any such run has ``complete = False`` and no cost.

``usage_from_logs`` reads the stream-json log a Claude-based driver left in its logs
dir, so the harness (main.py) can account for a run whose driver was killed.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

#: The stream-json log every Claude-based driver writes into its logs dir.
STREAM_LOG = "claude-code.txt"


@dataclass(frozen=True)
class RunUsage:
    """Totals of one agent run.

    ``input_tokens`` is uncached input only; the three input counts are disjoint, so
    ``total_input_tokens`` is their sum. ``total_cost_usd`` is None when unknown.
    ``result_subtype`` is the last result event's (``success``, ``error_max_turns``, ...).
    """

    input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    output_tokens: int = 0
    total_cost_usd: float | None = None
    num_turns: int = 0
    duration_ms: int = 0
    duration_api_ms: int = 0
    result_events: int = 0
    result_subtype: str = ""
    complete: bool = False
    source: str = ""

    @property
    def total_input_tokens(self) -> int:
        return self.input_tokens + self.cache_creation_input_tokens + self.cache_read_input_tokens

    def to_json(self) -> dict[str, object]:
        return {**asdict(self), "total_input_tokens": self.total_input_tokens}

    def result_fields(self, prefix: str = "usage.") -> dict[str, object]:
        """Flat fields for a results CSV row."""
        return {f"{prefix}{k}": v for k, v in self.to_json().items()}


def _int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _events(lines: Iterable[str]) -> Iterable[dict]:
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            yield event


def _model_usage_counts(event: Mapping) -> tuple[int, int, int, int] | None:
    """(uncached input, cache write, cache read, output) of a result's ``modelUsage``, or None if absent.

    ``modelUsage`` covers every model the session used (a subagent may run another one)
    and is cumulative over the session.
    """
    per_model = event.get("modelUsage")
    if not isinstance(per_model, dict) or not per_model:
        return None
    models = [m for m in per_model.values() if isinstance(m, dict)]
    return (
        sum(_int(m.get("inputTokens")) for m in models),
        sum(_int(m.get("cacheCreationInputTokens")) for m in models),
        sum(_int(m.get("cacheReadInputTokens")) for m in models),
        sum(_int(m.get("outputTokens")) for m in models),
    )


def _usage_counts(usage: object) -> tuple[int, int, int, int]:
    """The same four counts of a ``usage`` object (a result's, covering its own turns, or a message's)."""
    usage = usage if isinstance(usage, dict) else {}
    return (
        _int(usage.get("input_tokens")),
        _int(usage.get("cache_creation_input_tokens")),
        _int(usage.get("cache_read_input_tokens")),
        _int(usage.get("output_tokens")),
    )


@dataclass
class _Session:
    """Token totals of one session from its result events so far."""

    cumulative: tuple[int, int, int, int] = (0, 0, 0, 0)  # the last result's modelUsage
    since: tuple[int, int, int, int] = (0, 0, 0, 0)  # usage of later results without modelUsage
    cost: float | None = None  # the last result's total_cost_usd

    def add(self, event: Mapping) -> None:
        counts = _model_usage_counts(event)
        if counts is None:
            self.since = tuple(a + b for a, b in zip(self.since, _usage_counts(event.get("usage")), strict=True))
        else:
            self.cumulative, self.since = counts, (0, 0, 0, 0)
        cost = _float(event.get("total_cost_usd"))
        if cost is not None:
            self.cost = cost

    def counts(self) -> tuple[int, int, int, int]:
        return tuple(a + b for a, b in zip(self.cumulative, self.since, strict=True))


def parse_stream_json(lines: Iterable[str]) -> RunUsage:
    """A stream-json log's totals: each session's last result, plus the assistant messages of any run that has none."""
    sessions: dict[object, _Session] = {}
    turns = duration = api = results = 0
    subtype = ""
    open_runs = 0  # runs (init events) whose result event has not come yet
    pending: dict[str, Mapping] = {}  # assistant usage since the last result, per message id
    unfinished: list[Mapping] = []  # usage of runs that a later init showed never finished
    for event in _events(lines):
        kind = event.get("type")
        if kind == "system" and event.get("subtype") == "init":
            if open_runs:
                unfinished += pending.values()
            open_runs, pending = 1, {}
        elif kind == "result":
            open_runs, pending = 0, {}
            results += 1
            session_id = event.get("session_id")
            key = session_id if isinstance(session_id, str) and session_id else object()
            sessions.setdefault(key, _Session()).add(event)
            turns += _int(event.get("num_turns"))
            duration += _int(event.get("duration_ms"))
            api += _int(event.get("duration_api_ms"))
            subtype = str(event.get("subtype", ""))
        elif kind == "assistant":
            message = event.get("message")
            if isinstance(message, dict) and isinstance(message.get("usage"), dict):
                pending[str(message.get("id", len(pending)))] = message["usage"]
    unfinished += pending.values()
    counts = [sum(column) for column in zip((0, 0, 0, 0), *(s.counts() for s in sessions.values()), strict=True)]
    if results and not unfinished:
        costs = [s.cost for s in sessions.values() if s.cost is not None]
        cost = sum(costs) if costs else None
        return RunUsage(*counts, cost, turns, duration, api, results, subtype, True, "stream-json result")
    for usage in unfinished:
        for i, value in enumerate(_usage_counts(usage)):
            counts[i] += value
    source = "stream-json result + assistant messages" if results else "stream-json assistant messages"
    return RunUsage(
        *counts,
        num_turns=turns + len(unfinished),
        duration_ms=duration,
        duration_api_ms=api,
        result_events=results,
        result_subtype=subtype,
        source=source,
    )


def usage_from_logs(logs_dir: Path) -> RunUsage | None:
    """The usage a driver left in *logs_dir* (its stream-json log); None if there is none or it is unreadable.

    A containerized agent may leave its log owned by another user until the harness
    publishes the run's artifacts.
    """
    try:
        with open(logs_dir / STREAM_LOG, errors="replace") as f:
            return parse_stream_json(f)
    except OSError:
        return None
