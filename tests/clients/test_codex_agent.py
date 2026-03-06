"""
Unit tests for clients.codex.codex_agent (CodexAgent).
No CLI tool is invoked; subprocess calls are mocked throughout.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.codex.codex_agent import CodexAgent


class TestCodexAgentInit:
    def test_default_codex_home_is_logs_dir(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        assert agent.codex_home == tmp_path

    def test_custom_codex_home(self, tmp_path):
        home = tmp_path / "codex_home"
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o", codex_home=home)
        assert agent.codex_home == home
        assert home.is_dir()


class TestCodexAgentGetUsageMetrics:
    def test_empty_when_no_file(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        assert agent.get_usage_metrics() == {
            "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0
        }

    def test_parses_last_usage_event(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        agent.output_path.write_text(
            json.dumps({"type": "start"}) + "\n"
            + json.dumps({"usage": {"input_tokens": 300, "cached_input_tokens": 40, "output_tokens": 120}}) + "\n"
        )
        assert agent.get_usage_metrics() == {
            "input_tokens": 300, "cached_input_tokens": 40, "output_tokens": 120
        }

    def test_uses_last_usage_entry(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        agent.output_path.write_text(
            json.dumps({"usage": {"input_tokens": 1, "cached_input_tokens": 0, "output_tokens": 1}}) + "\n"
            + json.dumps({"usage": {"input_tokens": 999, "cached_input_tokens": 5, "output_tokens": 100}}) + "\n"
        )
        assert agent.get_usage_metrics()["input_tokens"] == 999

    def test_skips_non_json_lines(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        agent.output_path.write_text(
            "garbage\n"
            + json.dumps({"usage": {"input_tokens": 7, "cached_input_tokens": 0, "output_tokens": 3}}) + "\n"
        )
        assert agent.get_usage_metrics()["input_tokens"] == 7


class TestCodexAgentDoRun:
    def test_builds_correct_command(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.object(agent, "_setup_auth"):
                with patch.object(agent, "_cleanup_auth"):
                    agent._do_run("investigate", tmp_path / "env")

        cmd = mock_rs.call_args[0][0]
        assert cmd[0] == "codex"
        assert "exec" in cmd
        assert "--model" in cmd
        assert "gpt-4o" in cmd
        assert "--json" in cmd
        assert "investigate" in cmd

    def test_sets_codex_home_env(self, tmp_path):
        codex_home = tmp_path / "codex_home"
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o", codex_home=codex_home)
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.object(agent, "_setup_auth"):
                with patch.object(agent, "_cleanup_auth"):
                    agent._do_run("task", tmp_path / "env")
        assert mock_rs.call_args[0][2]["CODEX_HOME"] == str(codex_home)

    def test_cleanup_auth_called_even_on_exception(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        with patch.object(agent, "_setup_auth"):
            with patch.object(agent, "_cleanup_auth") as mock_cleanup:
                with patch.object(agent, "_run_subprocess", side_effect=RuntimeError("boom")):
                    with pytest.raises(RuntimeError):
                        agent._do_run("task", tmp_path / "env")
        mock_cleanup.assert_called_once()

    def test_setup_auth_writes_auth_json(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-openai-test"}):
            agent._setup_auth()
        auth = json.loads((tmp_path / "auth.json").read_text())
        assert auth["OPENAI_API_KEY"] == "sk-openai-test"

    def test_cleanup_auth_removes_auth_json(self, tmp_path):
        agent = CodexAgent(logs_dir=tmp_path, model_name="gpt-4o")
        (tmp_path / "auth.json").write_text("{}")
        agent._cleanup_auth()
        assert not (tmp_path / "auth.json").exists()
