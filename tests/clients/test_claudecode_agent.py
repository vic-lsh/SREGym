"""
Unit tests for clients.claudecode.claudecode_agent (ClaudeCodeAgent).
No CLI tool is invoked; subprocess calls are mocked throughout.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.claudecode.claudecode_agent import ClaudeCodeAgent


class TestClaudeCodeAgentInit:
    def test_creates_sessions_dir(self, tmp_path):
        sessions = tmp_path / "my_sessions"
        ClaudeCodeAgent(logs_dir=tmp_path, model_name="m", sessions_dir=sessions)
        assert sessions.is_dir()

    def test_default_sessions_dir_is_logs_subdir(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="m")
        assert agent.sessions_dir == tmp_path / "sessions"


class TestClaudeCodeAgentGetUsageMetrics:
    def _make_session(self, agent, events):
        session_dir = agent.sessions_dir / "projects" / "-app" / "session1"
        session_dir.mkdir(parents=True, exist_ok=True)
        (session_dir / "session.jsonl").write_text(
            "\n".join(json.dumps(e) for e in events) + "\n"
        )

    def test_empty_when_no_session_dir(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet")
        assert agent.get_usage_metrics() == {
            "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0
        }

    def test_parses_usage_from_message(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet")
        self._make_session(agent, [
            {"message": {"usage": {
                "input_tokens": 200,
                "cache_read_input_tokens": 50,
                "output_tokens": 80,
            }}},
        ])
        assert agent.get_usage_metrics() == {
            "input_tokens": 200, "cached_input_tokens": 50, "output_tokens": 80
        }

    def test_accumulates_across_multiple_messages(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet")
        self._make_session(agent, [
            {"message": {"usage": {"input_tokens": 10, "cache_read_input_tokens": 0, "output_tokens": 5}}},
            {"message": {"usage": {"input_tokens": 20, "cache_read_input_tokens": 3, "output_tokens": 9}}},
        ])
        assert agent.get_usage_metrics() == {
            "input_tokens": 30, "cached_input_tokens": 3, "output_tokens": 14
        }

    def test_skips_events_without_usage(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet")
        self._make_session(agent, [
            {"type": "tool_use"},
            {"message": {"usage": {"input_tokens": 7, "cache_read_input_tokens": 0, "output_tokens": 3}}},
        ])
        assert agent.get_usage_metrics()["input_tokens"] == 7


class TestClaudeCodeAgentDoRun:
    def test_builds_correct_command(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet-4-5")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
                agent._do_run("fix it", tmp_path / "env")

        cmd = mock_rs.call_args[0][0]
        assert cmd[0] == "claude"
        assert "stream-json" in cmd
        assert "fix it" in cmd
        assert "--allowedTools" in cmd

    def test_sets_anthropic_env_vars(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet-4-5")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-ant-test"}):
                agent._do_run("task", tmp_path / "env")

        env = mock_rs.call_args[0][2]
        assert env["ANTHROPIC_API_KEY"] == "sk-ant-test"
        assert env["CLAUDE_CONFIG_DIR"] == str(agent.sessions_dir)
        assert env["ANTHROPIC_MODEL"] == "claude-sonnet-4-5"

    def test_returns_1_when_no_auth(self, tmp_path):
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="claude-sonnet-4-5")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
            with patch("builtins.open", side_effect=FileNotFoundError):
                rc = agent._do_run("task", tmp_path / "env")
        assert rc == 1

    def test_falls_back_to_sonnet_for_non_anthropic_model(self, tmp_path):
        # split("/")[-1] must match an invalid pattern; "path/bedrock" → "bedrock"
        agent = ClaudeCodeAgent(logs_dir=tmp_path, model_name="path/bedrock")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
                agent._do_run("task", tmp_path / "env")
        assert mock_rs.call_args[0][2]["ANTHROPIC_MODEL"] == "sonnet"
