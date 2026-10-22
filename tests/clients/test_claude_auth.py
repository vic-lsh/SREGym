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
"""Claude auth: environment first, then the settings `env` block; Vertex credentials mounted alone."""

import json

import pytest

from clients.harness.claude_auth import (
    AUTH_KEYS,
    CONTAINER_ADC_PATH,
    CONTAINER_CREDENTIALS_PATH,
    ClaudeAuthError,
    auth_env,
    container_auth,
    missing_auth,
)
from llm_backend import init_backend
from sregym.service.container_runner import ContainerRunner

VERTEX = {"CLAUDE_CODE_USE_VERTEX": "1", "ANTHROPIC_VERTEX_PROJECT_ID": "proj", "CLOUD_ML_REGION": "global"}


def _config_dir(tmp_path, env):
    config = tmp_path / "claude-config"
    config.mkdir()
    (config / "settings.json").write_text(json.dumps({"env": env, "model": "ignored"}))
    return config


def test_settings_fill_only_auth_keys_the_environment_lacks(tmp_path):
    config = _config_dir(tmp_path, {**VERTEX, "ANTHROPIC_MODEL": "x", "VERTEX_REGION_CLAUDE_X": "us"})
    auth = auth_env({"CLAUDE_CONFIG_DIR": str(config), "CLOUD_ML_REGION": "us-east5", "HOME": "/h"})
    assert auth == {**VERTEX, "CLOUD_ML_REGION": "us-east5", "VERTEX_REGION_CLAUDE_X": "us"}


def test_the_auth_config_dir_wins_over_claude_config_dir(tmp_path):
    config = _config_dir(tmp_path, VERTEX)
    environ = {"SREGYM_CLAUDE_AUTH_CONFIG_DIR": str(config), "CLAUDE_CONFIG_DIR": str(tmp_path / "none")}
    assert auth_env(environ) == VERTEX


def test_a_malformed_settings_file_is_refused(tmp_path):
    config = tmp_path / "c"
    config.mkdir()
    (config / "settings.json").write_text("[]")
    with pytest.raises(ClaudeAuthError):
        auth_env({"CLAUDE_CONFIG_DIR": str(config)})


@pytest.mark.parametrize(
    ("auth", "ok"),
    [
        ({"ANTHROPIC_API_KEY": "k"}, True),
        ({"CLAUDE_CODE_OAUTH_TOKEN": "t"}, True),
        (VERTEX, True),
        ({**VERTEX, "CLAUDE_CODE_USE_VERTEX": "0"}, False),
        ({k: v for k, v in VERTEX.items() if k != "CLOUD_ML_REGION"}, False),
        ({k: v for k, v in VERTEX.items() if k != "ANTHROPIC_VERTEX_PROJECT_ID"}, False),
        ({}, False),
    ],
)
def test_claude_code_needs_a_key_a_token_or_a_whole_vertex_setup(auth, ok):
    assert (missing_auth(auth) is None) == ok


def test_a_credentials_file_is_mounted_alone_read_only(tmp_path):
    credentials = tmp_path / "sa.json"
    credentials.write_text("{}")
    env, mounts = container_auth({**VERTEX, "GOOGLE_APPLICATION_CREDENTIALS": str(credentials)}, tmp_path)
    assert env["GOOGLE_APPLICATION_CREDENTIALS"] == CONTAINER_CREDENTIALS_PATH
    assert mounts == [(credentials.resolve(), CONTAINER_CREDENTIALS_PATH)]


def test_a_missing_credentials_file_is_an_error(tmp_path):
    with pytest.raises(ClaudeAuthError, match="does not exist"):
        container_auth({"GOOGLE_APPLICATION_CREDENTIALS": str(tmp_path / "gone.json")}, tmp_path)


def test_vertex_without_a_credentials_file_uses_application_default_credentials(tmp_path):
    adc = tmp_path / ".config" / "gcloud" / "application_default_credentials.json"
    adc.parent.mkdir(parents=True)
    adc.write_text("{}")
    assert container_auth(VERTEX, tmp_path)[1] == [(adc.resolve(), CONTAINER_ADC_PATH)]
    assert container_auth({"ANTHROPIC_API_KEY": "k"}, tmp_path)[1] == []


def test_the_claude_code_container_gets_the_auth_env_and_only_the_credentials_mount(tmp_path, monkeypatch):
    credentials = tmp_path / "sa.json"
    credentials.write_text("{}")
    config = _config_dir(tmp_path, {**VERTEX, "GOOGLE_APPLICATION_CREDENTIALS": str(credentials)})
    for key in (*AUTH_KEYS, "SREGYM_CLAUDE_AUTH_CONFIG_DIR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))
    monkeypatch.setenv("HOME", str(tmp_path))
    runner = ContainerRunner()
    flags = runner._build_env_flags(agent="claudecode")
    for key, value in {**VERTEX, "GOOGLE_APPLICATION_CREDENTIALS": CONTAINER_CREDENTIALS_PATH}.items():
        assert f"{key}={value}" in flags
    args = runner._build_base_docker_args(agent="claudecode")
    mounts = [args[i + 1] for i, arg in enumerate(args) if arg == "-v"]
    assert f"{credentials.resolve()}:{CONTAINER_CREDENTIALS_PATH}:ro" in mounts
    assert not any(str(config) in mount for mount in mounts)


@pytest.mark.parametrize(
    ("model", "environ", "vertex_model"),
    [
        ("claude-opus-5-5", VERTEX, "claude-opus-5-5"),
        ("vertex_ai/claude-opus-5-5", VERTEX, "claude-opus-5-5"),
        ("vertex_ai/claude-opus-5-5", {"GOOGLE_CLOUD_PROJECT": "proj"}, "claude-opus-5-5"),
        ("anthropic/claude-opus-5-5", VERTEX, None),
        ("claude-opus-5-5", {"ANTHROPIC_API_KEY": "k"}, None),
        # A project for other agents (geminicli, opencode) is not Claude Code's Vertex switch.
        ("claude-opus-5-5", {"GOOGLE_CLOUD_PROJECT": "proj", "ANTHROPIC_API_KEY": "k"}, None),
        ("claude-opus-5-5", {"ANTHROPIC_VERTEX_PROJECT_ID": "proj", "CLOUD_ML_REGION": "global"}, None),
        ("claude-opus-5-5", {"CLAUDE_CODE_USE_VERTEX": "1"}, None),
        ("claude-opus-5-5", {**VERTEX, "JUDGE_API_BASE": "http://proxy"}, None),
        ("gpt-5", VERTEX, None),
        ("openrouter/claude-opus-5-5", VERTEX, None),
    ],
)
def test_a_claude_judge_uses_vertex_when_claude_code_would(monkeypatch, model, environ, vertex_model):
    for key in (*AUTH_KEYS, "JUDGE_API_BASE", "CLAUDE_CONFIG_DIR", "SREGYM_CLAUDE_AUTH_CONFIG_DIR"):
        monkeypatch.delenv(key, raising=False)
    for key, value in environ.items():
        monkeypatch.setenv(key, value)
    assert init_backend._vertex_claude_model(model) == vertex_model
