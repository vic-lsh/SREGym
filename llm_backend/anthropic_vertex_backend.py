"""Claude on Vertex AI through the Anthropic SDK, for the LLM-as-a-judge.

Drop-in for ``LiteLLMBackend`` (``inference(messages) -> obj`` with a ``.content`` string), from
robust-skills' SREGym. It authenticates the way Claude Code does on Vertex
(``ANTHROPIC_VERTEX_PROJECT_ID`` and ``CLOUD_ML_REGION``, Google credentials), taken from the
environment or, for any not set there, from the Claude settings fallback
(``clients.harness.claude_auth``). ``temperature`` is sent only when set; Claude models that
deprecate it reject it.
"""

from __future__ import annotations

import logging
import os
import time
from types import SimpleNamespace

from clients.harness.claude_auth import auth_env, vertex_project

logger = logging.getLogger("all.llm_backend.anthropic_vertex")

_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class AnthropicVertexBackend:
    def __init__(
        self,
        model_name: str,
        region: str | None = None,
        project_id: str | None = None,
        temperature: float | None = None,
        max_tokens: int = 8192,
        max_retries: int = 4,
    ):
        from anthropic import AnthropicVertex

        auth = auth_env(os.environ)
        self.model_name = model_name
        self.region = region or os.environ.get("ANTHROPIC_VERTEX_REGION") or auth.get("CLOUD_ML_REGION") or "global"
        self.project_id = project_id or vertex_project(auth) or os.environ.get("VERTEXAI_PROJECT")
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        credentials = None
        # The SDK finds GOOGLE_APPLICATION_CREDENTIALS in the environment by itself; a file named
        # only by the settings fallback is loaded here.
        credentials_file = auth.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_file and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            import google.auth

            credentials, _ = google.auth.load_credentials_from_file(credentials_file, scopes=[_CLOUD_PLATFORM_SCOPE])
        self._client = AnthropicVertex(region=self.region, project_id=self.project_id, credentials=credentials)
        logger.info(f"AnthropicVertexBackend ready: model={self.model_name} region={self.region}")

    def _convert(self, messages, system_prompt=None):
        """Convert langchain/str/dict messages -> (system_str, anthropic_messages)."""
        try:
            from langchain_core.messages import AIMessage, SystemMessage
        except Exception:  # pragma: no cover
            AIMessage = SystemMessage = None

        system_parts = []
        if system_prompt:
            system_parts.append(system_prompt)
        out = []

        if isinstance(messages, str):
            out.append({"role": "user", "content": messages})
        else:
            for m in messages:
                if isinstance(m, dict):
                    role = m.get("role", "user")
                    content = m.get("content", "")
                    if role == "system":
                        system_parts.append(content)
                    else:
                        out.append({"role": "assistant" if role == "assistant" else "user", "content": content})
                    continue
                content = getattr(m, "content", "")
                if SystemMessage is not None and isinstance(m, SystemMessage):
                    system_parts.append(content)
                elif AIMessage is not None and isinstance(m, AIMessage):
                    out.append({"role": "assistant", "content": content})
                else:  # HumanMessage or anything else -> user
                    out.append({"role": "user", "content": content})

        if not out:
            out.append({"role": "user", "content": ""})
        return "\n\n".join(p for p in system_parts if p), out

    def inference(self, messages, system_prompt=None, tools=None):
        system_str, msgs = self._convert(messages, system_prompt)
        kwargs = dict(model=self.model_name, max_tokens=self.max_tokens, messages=msgs)
        if self.temperature is not None:
            kwargs["temperature"] = self.temperature
        if system_str:
            kwargs["system"] = system_str

        last_exc = None
        for attempt in range(self.max_retries):
            try:
                resp = self._client.messages.create(**kwargs)
                text = "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", "") == "text")
                if not text and resp.content:
                    text = getattr(resp.content[0], "text", "") or ""
                return SimpleNamespace(content=text)
            except Exception as e:  # transient (429/503) or otherwise: retry with backoff
                last_exc = e
                logger.warning(
                    f"AnthropicVertex judge inference failed "
                    f"(attempt {attempt + 1}/{self.max_retries}): {type(e).__name__}: {str(e)[:140]}"
                )
                time.sleep(min(2**attempt, 20))
        raise last_exc
