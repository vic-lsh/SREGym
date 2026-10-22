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
"""Judge defaults: one call, the backend's own temperature, and a Claude-on-Vertex request without one."""

from types import SimpleNamespace

import pytest

from llm_backend.anthropic_vertex_backend import AnthropicVertexBackend, accepts_temperature
from sregym.conductor.oracles.llm_as_a_judge.judge import JudgmentResult
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle


def test_one_round_by_default_leaves_the_backend_temperature_alone(monkeypatch):
    monkeypatch.delenv("JUDGE_NUM_ROUNDS", raising=False)
    backend = SimpleNamespace(temperature=None)
    seen = []

    def judge_detailed(solution, expectation, other_faults=()):
        seen.append(backend.temperature)
        return SimpleNamespace(verdict=None, reasoning="", composite_score=0.0, dimensions=[])

    oracle = LLMAsAJudgeOracle(problem=None, expected="cause", backend=backend)
    oracle.judge.judge_detailed = judge_detailed
    oracle.evaluate("diagnosis")
    assert oracle.num_rounds == 1
    assert seen == [None]


def _vertex_backend(model_name, sent):
    def create(**kwargs):
        sent.append(kwargs)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text="ok")])

    backend = object.__new__(AnthropicVertexBackend)
    backend.model_name, backend.temperature, backend.max_tokens, backend.max_retries = model_name, None, 8192, 1
    backend._client = SimpleNamespace(messages=SimpleNamespace(create=create))
    return backend


def test_vertex_requests_send_no_temperature_and_8192_max_tokens():
    sent = []
    backend = _vertex_backend("claude-opus-5-5", sent)
    assert backend.inference("Say ok.", system_prompt="Reply ok.").content == "ok"
    assert sent == [
        {
            "model": "claude-opus-5-5",
            "max_tokens": 8192,
            "messages": [{"role": "user", "content": "Say ok."}],
            "system": "Reply ok.",
        }
    ]


@pytest.mark.parametrize(
    ("model_name", "accepted"),
    [
        ("claude-sonnet-4-6", True),
        ("claude-sonnet-4-6-20250627", True),
        ("claude-opus-4-5@20251101", True),
        ("claude-haiku-4-5", True),
        ("claude-3-7-sonnet@20250219", True),
        ("claude-opus-4-7", False),
        ("claude-opus-4-8", False),
        ("claude-opus-5-5", False),
        ("claude-sonnet-5-5", False),
        ("claude-haiku-5-5", False),
        ("claude-fable-5-1", False),
    ],
)
def test_temperature_is_only_for_models_known_to_accept_it(model_name, accepted):
    assert accepts_temperature(model_name) is accepted


@pytest.mark.parametrize(("model_name", "temperature"), [("claude-opus-5-5", None), ("claude-sonnet-4-6", 0.7)])
def test_voting_rounds_on_vertex_send_temperature_only_where_accepted(monkeypatch, model_name, temperature):
    monkeypatch.delenv("JUDGE_VOTING_TEMPERATURE", raising=False)
    sent = []
    backend = _vertex_backend(model_name, sent)
    verdicts = iter([True, False, True])

    def judge_detailed(solution, expectation, other_faults=()):
        backend.inference("judge this")
        return SimpleNamespace(
            verdict=JudgmentResult.TRUE if next(verdicts) else JudgmentResult.FALSE,
            reasoning="",
            composite_score=0.0,
            dimensions=[],
        )

    oracle = LLMAsAJudgeOracle(problem=None, expected="cause", backend=backend, num_rounds=3)
    oracle.judge.judge_detailed = judge_detailed
    result = oracle.evaluate("diagnosis")

    assert result["success"] is True
    assert result["vote_count"] == "2/3"
    assert [request.get("temperature") for request in sent] == [temperature] * 3
    assert backend.temperature is None
