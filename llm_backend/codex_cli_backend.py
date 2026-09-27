"""LLM-as-a-judge backend that answers through the locally logged-in Codex CLI.

Selected with ``JUDGE_MODEL_ID=codex-<model>`` (for example ``codex-gpt-6-astra``).
Reasoning effort comes from ``JUDGE_REASONING_EFFORT`` (default ``xhigh``).
Each call is an ephemeral, read-only ``codex exec`` in an empty directory, so
the judge sees only the prompt and never the cluster or the agent workspace.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path

from langchain_core.messages import AIMessage, BaseMessage

logger = logging.getLogger(__name__)

CODEX_JUDGE_PREFIX = "codex-"
DEFAULT_JUDGE_REASONING_EFFORT = "xhigh"
DEFAULT_TIMEOUT_SECONDS = 900


class CodexCLIBackend:
    def __init__(self, model: str, reasoning_effort: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS):
        if not model.strip():
            raise ValueError("Codex judge model must not be empty")
        if not reasoning_effort.strip():
            raise ValueError("Codex judge reasoning effort must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("Codex judge timeout must be positive")
        self.model = model.strip()
        self.reasoning_effort = reasoning_effort.strip()
        self.timeout_seconds = timeout_seconds

    def _prompt(self, messages: str | list[BaseMessage], system_prompt: str | None) -> str:
        if isinstance(messages, str):
            parts = [system_prompt] if system_prompt else []
            parts.append(messages)
        else:
            parts = [f"[{message.type}]\n{message.content}" for message in messages]
            if system_prompt:
                parts.insert(0, f"[system]\n{system_prompt}")
        return (
            "You are acting as an evaluator. Do not run commands or inspect files; "
            "answer only from the text below, in exactly the requested output format.\n\n" + "\n\n".join(parts)
        )

    def inference(
        self,
        messages: str | list[BaseMessage],
        system_prompt: str | None = None,
        tools: list | None = None,
    ) -> AIMessage:
        if tools:
            raise ValueError("Codex judge backend does not support tool binding")
        prompt = self._prompt(messages, system_prompt)
        with tempfile.TemporaryDirectory(prefix="sregym-codex-judge-") as workdir:
            output = Path(workdir) / "last-message.txt"
            command = [
                "codex",
                "exec",
                "--model",
                self.model,
                "-c",
                f"model_reasoning_effort={self.reasoning_effort}",
                "--sandbox",
                "read-only",
                "--ephemeral",
                "--skip-git-repo-check",
                "--cd",
                workdir,
                "--output-last-message",
                str(output),
                "-",
            ]
            completed = subprocess.run(
                command,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )
            if completed.returncode != 0:
                details = (completed.stderr or completed.stdout or "").strip()[-2000:]
                raise RuntimeError(f"codex judge ({self.model}) exited {completed.returncode}: {details}")
            text = output.read_text(encoding="utf-8").strip() if output.is_file() else ""
        if not text:
            raise RuntimeError(f"codex judge ({self.model}) returned no final message")
        logger.info("codex judge %s/%s answered (%d chars)", self.model, self.reasoning_effort, len(text))
        return AIMessage(content=text)
