import os

from clients.harness.claude_auth import auth_env, vertex_project
from llm_backend.codex_cli_backend import CODEX_JUDGE_PREFIX, DEFAULT_JUDGE_REASONING_EFFORT, CodexCLIBackend
from llm_backend.get_llm_backend import LiteLLMBackend


def get_llm_backend(
    model_name: str,
    api_base: str | None = None,
    api_key: str | None = None,
) -> LiteLLMBackend:
    """Initialize an LLM backend for the given litellm model string."""
    endpoint_status = "set" if api_base else "unset"
    print(f"🔧 Initializing LLM backend — model: {model_name}, api_base: {endpoint_status}")
    return LiteLLMBackend(model_name=model_name, api_base=api_base, api_key=api_key)


def get_llm_backend_for_agent() -> LiteLLMBackend:
    """Get LLM backend for agent tasks"""
    model_id = os.environ.get("AGENT_MODEL_ID")
    if not model_id:
        raise ValueError("AGENT_MODEL_ID environment variable is not set.")
    return get_llm_backend(
        model_id,
        api_base=os.environ.get("AGENT_API_BASE"),
        api_key=os.environ.get("AGENT_API_KEY"),
    )


def _vertex_claude_model(model_id: str) -> str | None:
    """The bare Claude model name when the judge should use Claude on Vertex, else None.

    A Claude model (no provider prefix, or ``anthropic/`` or ``vertex_ai/``) goes to Vertex when a
    Vertex project is configured, in the environment or the Claude settings fallback. Otherwise
    the model stays on LiteLLM (e.g. Anthropic's API with ANTHROPIC_API_KEY).
    """
    provider, _, bare = model_id.rpartition("/")
    if provider not in ("", "anthropic", "vertex_ai") or not bare.lower().startswith("claude"):
        return None
    if os.environ.get("JUDGE_API_BASE") or not vertex_project(auth_env(os.environ)):
        return None
    return bare


def get_llm_backend_for_judge():
    """Get LLM backend for the LLM-as-a-judge evaluator."""
    model_id = os.environ.get("JUDGE_MODEL_ID")
    if not model_id:
        raise ValueError("JUDGE_MODEL_ID environment variable is not set.")
    if vertex_model := _vertex_claude_model(model_id):
        from llm_backend.anthropic_vertex_backend import AnthropicVertexBackend

        print(f"🔧 Initializing Claude-on-Vertex judge — model: {vertex_model}")
        return AnthropicVertexBackend(model_name=vertex_model)
    if model_id.startswith(CODEX_JUDGE_PREFIX):
        effort = os.environ.get("JUDGE_REASONING_EFFORT", "").strip() or DEFAULT_JUDGE_REASONING_EFFORT
        print(f"🔧 Initializing Codex CLI judge — model: {model_id}, reasoning_effort: {effort}")
        return CodexCLIBackend(model=model_id.removeprefix(CODEX_JUDGE_PREFIX), reasoning_effort=effort)
    return get_llm_backend(
        model_id,
        api_base=os.environ.get("JUDGE_API_BASE"),
        api_key=os.environ.get("JUDGE_API_KEY"),
    )
