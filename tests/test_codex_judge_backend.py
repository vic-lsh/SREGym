import subprocess
from pathlib import Path

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from llm_backend import init_backend
from llm_backend.codex_cli_backend import CodexCLIBackend


def test_codex_judge_id_selects_codex_cli_backend(monkeypatch) -> None:
    monkeypatch.setenv("JUDGE_MODEL_ID", "codex-gpt-6-astra")
    monkeypatch.setenv("JUDGE_REASONING_EFFORT", "xhigh")

    backend = init_backend.get_llm_backend_for_judge()

    assert isinstance(backend, CodexCLIBackend)
    assert backend.model == "gpt-6-astra"
    assert backend.reasoning_effort == "xhigh"


def test_codex_backend_runs_read_only_ephemeral_exec_and_returns_last_message(monkeypatch) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        output = Path(command[command.index("--output-last-message") + 1])
        output.write_text('{"judgment": "True", "reasoning": "ok"}\n', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    backend = CodexCLIBackend(model="gpt-6-astra", reasoning_effort="xhigh")

    response = backend.inference([SystemMessage(content="judge rules"), HumanMessage(content="answer")])

    assert response.content == '{"judgment": "True", "reasoning": "ok"}'
    command, kwargs = calls[0]
    assert command[:2] == ["codex", "exec"]
    assert command[command.index("--model") + 1] == "gpt-6-astra"
    assert "model_reasoning_effort=xhigh" in command
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert "--ephemeral" in command
    assert "judge rules" in kwargs["input"] and "answer" in kwargs["input"]


def test_codex_backend_raises_with_context_on_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 1, stdout="", stderr="model not supported"),
    )
    backend = CodexCLIBackend(model="gpt-6-astra", reasoning_effort="xhigh")

    with pytest.raises(RuntimeError, match="model not supported"):
        backend.inference("answer")


def test_codex_backend_rejects_empty_model() -> None:
    with pytest.raises(ValueError):
        CodexCLIBackend(model="", reasoning_effort="xhigh")
