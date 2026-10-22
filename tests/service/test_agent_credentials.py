"""Each containerized agent receives only the credentials it needs (AGENT_CREDENTIALS)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from clients.harness.claude_auth import AUTH_KEYS, CONTAINER_CREDENTIALS_PATH, ClaudeAuthError
from sregym.service.container_runner import (
    AGENT_CREDENTIALS,
    COMMON_VARS,
    ContainerConfig,
    ContainerRunner,
    ExecInput,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

HOST_SECRETS = {
    "OPENAI_API_KEY": "openai",
    "ANTHROPIC_API_KEY": "anthropic",
    "GEMINI_API_KEY": "gemini",
    "AWS_ACCESS_KEY_ID": "aws",
    "COPILOT_GITHUB_TOKEN": "copilot",
    "AZURE_API_KEY": "azure",
    "AGENT_API_KEY": "agent",
}


@pytest.fixture
def host(monkeypatch, tmp_path):
    """A host with every provider's credentials: env keys, ~/.aws, Codex auth and a Google key file."""
    home = tmp_path / "home"
    (home / ".aws").mkdir(parents=True)
    (home / ".codex").mkdir()
    (home / ".codex" / "auth.json").write_text("{}")
    google = tmp_path / "sa.json"
    google.write_text("{}")
    for key in (*AUTH_KEYS, "CLAUDE_CONFIG_DIR", "SREGYM_CLAUDE_AUTH_CONFIG_DIR", "KUBECONFIG"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(home))
    for key, value in {**HOST_SECRETS, "GOOGLE_APPLICATION_CREDENTIALS": str(google), "AGENT_MODEL_ID": "m"}.items():
        monkeypatch.setenv(key, value)


def _container(agent: str, tmp_path: Path) -> tuple[dict[str, str], list[str]]:
    runner = ContainerRunner(ContainerConfig(logs_path=tmp_path / "logs"))
    runner._image_id = "sha256:checked"
    try:
        cmd = runner.build_docker_command(ExecInput(command="true", agent=agent))
    finally:
        runner.cleanup_credential_tmps()
    env = dict(cmd[i + 1].split("=", 1) for i, arg in enumerate(cmd[:-1]) if arg == "-e")
    mounts = [cmd[i + 1] for i, arg in enumerate(cmd[:-1]) if arg == "-v"]
    return env, mounts


def _credentials(env: dict[str, str]) -> set[str]:
    return set(env) - set(COMMON_VARS) - {"KUBECONFIG", "AGENT_LOGS_DIR"}


def _credential_mounts(mounts: list[str]) -> set[str]:
    return {m.split(":")[1] for m in mounts} - {"/logs", "/workspace", "/root/.kube/config"}


def test_claude_code_gets_only_its_claude_auth(host, tmp_path):
    env, mounts = _container("claudecode", tmp_path)

    assert _credentials(env) == {"ANTHROPIC_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"}
    assert env["GOOGLE_APPLICATION_CREDENTIALS"] == CONTAINER_CREDENTIALS_PATH
    assert _credential_mounts(mounts) == {CONTAINER_CREDENTIALS_PATH}


@pytest.mark.parametrize(
    ("agent", "credentials", "credential_mounts"),
    [
        ("codex", {"OPENAI_API_KEY"}, {"/root/.codex"}),
        ("gemini", {"GEMINI_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"}, {CONTAINER_CREDENTIALS_PATH}),
        ("copilot", {"COPILOT_GITHUB_TOKEN"}, set()),
        ("autosubmit", set(), set()),
        (
            "stratus",
            {*HOST_SECRETS} - {"COPILOT_GITHUB_TOKEN"} | {"GOOGLE_APPLICATION_CREDENTIALS"},
            {"/root/.aws", CONTAINER_CREDENTIALS_PATH},
        ),
    ],
)
def test_other_agents_get_only_their_provider_credentials(host, tmp_path, agent, credentials, credential_mounts):
    env, mounts = _container(agent, tmp_path)

    assert _credentials(env) == credentials
    assert _credential_mounts(mounts) == credential_mounts
    assert all(env[key] == value for key, value in HOST_SECRETS.items() if key in env)


@pytest.mark.parametrize("breakage", ["missing credentials file", "malformed settings.json"])
def test_broken_claude_auth_blocks_only_claude_code(host, tmp_path, monkeypatch, breakage):
    if breakage == "missing credentials file":
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "gone.json"))
    else:
        config = tmp_path / "claude"
        config.mkdir()
        (config / "settings.json").write_text("{not json")
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))

    for agent in ("stratus", "codex", "gemini", "copilot", "opencode", "autosubmit"):
        env, _ = _container(agent, tmp_path)
        assert not any(key.startswith(("CLAUDE_CODE_", "ANTHROPIC_VERTEX")) for key in env)
    with pytest.raises(ClaudeAuthError):
        _container("claudecode", tmp_path)


def test_every_containerized_agent_has_a_credential_allow_list():
    agents = yaml.safe_load((REPO_ROOT / "agents.yaml").read_text())["agents"]
    containerized = {a["name"] for a in agents if a.get("container_isolation", True) and a.get("kickoff_command")}
    assert containerized <= set(AGENT_CREDENTIALS)
