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
"""Model-provider auth for Claude Code and the Claude judge: API key, OAuth token, or Vertex.

Standard library only. The auth variables come from the caller's environment and, for any not
set there, from the ``env`` block of ``$SREGYM_CLAUDE_AUTH_CONFIG_DIR/settings.json`` (default
``$CLAUDE_CONFIG_DIR``). Nothing else of that directory is read. Same rules as robust-skills'
agent sandbox (``clients/harness/sandbox.py`` there).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

#: Variables that carry model-provider auth.
AUTH_KEYS = (
    "CLAUDE_CODE_USE_VERTEX",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLOUD_ML_REGION",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
)
#: Per-model Vertex region overrides (``VERTEX_REGION_CLAUDE_...``) are auth too.
AUTH_PREFIXES = ("VERTEX_REGION_",)

#: Names the source config dir for the settings fallback; defaults to ``CLAUDE_CONFIG_DIR``.
AUTH_CONFIG_DIR_ENV = "SREGYM_CLAUDE_AUTH_CONFIG_DIR"

#: Where an agent container sees the Google credentials file (mounted read-only, alone).
CONTAINER_CREDENTIALS_PATH = "/opt/sregym-auth/gcp-credentials.json"
#: Where an agent container sees gcloud application-default credentials (the fallback).
CONTAINER_ADC_PATH = "/root/.config/gcloud/application_default_credentials.json"


class ClaudeAuthError(ValueError):
    """The auth settings are malformed or name a credentials file that does not exist."""


def is_auth_key(key: str) -> bool:
    return key in AUTH_KEYS or key.startswith(AUTH_PREFIXES)


def settings_env(config_dir: Path | None) -> dict[str, str]:
    """The ``env`` block of ``config_dir/settings.json``; empty if there is none.

    Raises:
        ClaudeAuthError: The file exists but is not a JSON object with an object ``env``.
    """
    if config_dir is None:
        return {}
    path = config_dir / "settings.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise ClaudeAuthError(f"{path}: not JSON ({e})") from None
    env = data.get("env", {}) if isinstance(data, dict) else None
    if not isinstance(env, dict):
        raise ClaudeAuthError(f"{path}: want a JSON object with an object 'env'")
    return {str(k): str(v) for k, v in env.items()}


def auth_config_dir(environ: Mapping[str, str]) -> Path | None:
    """``$SREGYM_CLAUDE_AUTH_CONFIG_DIR``, else ``$CLAUDE_CONFIG_DIR``, else None."""
    raw = environ.get(AUTH_CONFIG_DIR_ENV) or environ.get("CLAUDE_CONFIG_DIR")
    return Path(raw) if raw else None


def auth_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The non-empty auth variables: *environ*'s, then the settings fallback's for any it lacks."""
    found = {k: v for k, v in settings_env(auth_config_dir(environ)).items() if is_auth_key(k) and v}
    found.update({k: v for k, v in environ.items() if is_auth_key(k) and v})
    return found


def uses_vertex(auth: Mapping[str, str]) -> bool:
    """Claude Code's switch: ``CLAUDE_CODE_USE_VERTEX`` set to anything but empty, 0 or false."""
    return auth.get("CLAUDE_CODE_USE_VERTEX", "").strip().lower() not in ("", "0", "false", "no")


def vertex_project(auth: Mapping[str, str]) -> str | None:
    return auth.get("ANTHROPIC_VERTEX_PROJECT_ID") or auth.get("GOOGLE_CLOUD_PROJECT") or None


def missing_auth(auth: Mapping[str, str]) -> str | None:
    """Why Claude Code cannot authenticate with *auth*, or None when it can."""
    if auth.get("ANTHROPIC_API_KEY") or auth.get("CLAUDE_CODE_OAUTH_TOKEN"):
        return None
    if not uses_vertex(auth):
        return "set ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN, or CLAUDE_CODE_USE_VERTEX=1 with a Vertex project"
    if not vertex_project(auth):
        return "CLAUDE_CODE_USE_VERTEX is set but ANTHROPIC_VERTEX_PROJECT_ID is not"
    if not auth.get("CLOUD_ML_REGION"):
        return "CLAUDE_CODE_USE_VERTEX is set but CLOUD_ML_REGION is not"
    return None


def container_auth(environ: Mapping[str, str], home: Path) -> tuple[dict[str, str], list[tuple[Path, str]]]:
    """Auth for an agent container: ``(env, read-only file mounts as (host path, container path))``.

    A ``GOOGLE_APPLICATION_CREDENTIALS`` file is mounted alone at ``CONTAINER_CREDENTIALS_PATH``
    and the variable points there. Without one, Vertex auth falls back to the caller's gcloud
    application-default credentials file under *home*, if it exists.

    Raises:
        ClaudeAuthError: The settings file is malformed, or the named credentials file is missing.
    """
    env = auth_env(environ)
    mounts: list[tuple[Path, str]] = []
    credentials = env.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    if credentials:
        path = Path(credentials)
        if not path.is_file():
            raise ClaudeAuthError(f"GOOGLE_APPLICATION_CREDENTIALS file {credentials} does not exist")
        mounts.append((path.resolve(), CONTAINER_CREDENTIALS_PATH))
        env["GOOGLE_APPLICATION_CREDENTIALS"] = CONTAINER_CREDENTIALS_PATH
    elif uses_vertex(env):
        adc = home / ".config" / "gcloud" / "application_default_credentials.json"
        if adc.is_file():
            mounts.append((adc.resolve(), CONTAINER_ADC_PATH))
    return env, mounts
