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

from llm_backend.anthropic_vertex_backend import AnthropicVertexBackend
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


def test_vertex_requests_send_no_temperature_and_8192_max_tokens():
    sent = {}

    def create(**kwargs):
        sent.update(kwargs)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text="ok")])

    backend = object.__new__(AnthropicVertexBackend)
    backend.model_name, backend.temperature, backend.max_tokens, backend.max_retries = "claude-opus-5-5", None, 8192, 1
    backend._client = SimpleNamespace(messages=SimpleNamespace(create=create))
    assert backend.inference("Say ok.", system_prompt="Reply ok.").content == "ok"
    assert sent == {
        "model": "claude-opus-5-5",
        "max_tokens": 8192,
        "messages": [{"role": "user", "content": "Say ok."}],
        "system": "Reply ok.",
    }
