import logging

import litellm

logger = logging.getLogger(__name__)


def _is_gemini(model_name: str) -> bool:
    bare = model_name.split("/")[-1].lower()
    return bare.startswith("gemini")


def get_context_window(model_name: str) -> int:
    """Return the context window size for *model_name* via litellm.

    For Gemini models, also tries the vertex_ai/ prefix if the bare name fails.
    Raises ValueError if the model is unknown or the info is unavailable.
    """
    candidates = [model_name]
    if _is_gemini(model_name) and not model_name.startswith("vertex_ai/"):
        bare = model_name.split("/")[-1]
        candidates.append(f"vertex_ai/{bare}")

    last_exc = None
    for candidate in candidates:
        try:
            info = litellm.get_model_info(candidate)
            window = info.get("max_input_tokens") or info.get("max_tokens")
            if window is not None:
                return int(window)
        except Exception as e:
            last_exc = e

    raise ValueError(
        f"Could not retrieve model info for '{model_name}': {last_exc}"
    )


def count_tokens(model_name: str, messages: list) -> int:
    """Estimate token count for *messages* using litellm.

    Falls back to a character-based estimate if litellm raises.
    """
    _ROLE_MAP = {"human": "user", "ai": "assistant", "system": "system", "tool": "tool"}

    msg_dicts = []
    for m in messages:
        role = getattr(m, "type", None) or getattr(m, "role", "user")
        role = _ROLE_MAP.get(role, role)
        content = m.content if hasattr(m, "content") else str(m)
        msg_dicts.append({"role": role, "content": content if isinstance(content, str) else str(content)})

    try:
        return litellm.token_counter(model=model_name, messages=msg_dicts)
    except Exception:
        return sum(len(str(m.content)) for m in messages) // 4
