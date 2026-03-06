"""
Unit tests for clients.gemini_cli.gemini_cli_agent (GeminiCliAgent).
No CLI tool is invoked; subprocess calls are mocked throughout.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.gemini_cli.gemini_cli_agent import GeminiCliAgent


class TestGeminiCliAgentInit:
    def test_strips_vertex_ai_prefix_from_model(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="vertex-ai-gemini-2.0-flash")
        assert agent.model_name == "gemini-2.0-flash"

    def test_creates_sessions_dir(self, tmp_path):
        sessions = tmp_path / "my_sessions"
        GeminiCliAgent(logs_dir=tmp_path, model_name="m", sessions_dir=sessions)
        assert sessions.is_dir()

    def test_default_sessions_dir_is_logs_subdir(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="m")
        assert agent.sessions_dir == tmp_path / "sessions"


class TestGeminiCliAgentGetUsageMetrics:
    def _write_output(self, path, events):
        path.write_text("\n".join(json.dumps(e) for e in events) + "\n")

    def test_empty_when_no_file(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        assert agent.get_usage_metrics() == {
            "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0
        }

    def test_parses_result_event(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        self._write_output(agent.output_path, [
            {"type": "text", "content": "thinking..."},
            {"type": "result", "stats": {"input_tokens": 100, "output_tokens": 50, "cached": 20}},
        ])
        assert agent.get_usage_metrics() == {
            "input_tokens": 100, "output_tokens": 50, "cached_input_tokens": 20
        }

    def test_accumulates_multiple_result_events(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        self._write_output(agent.output_path, [
            {"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5, "cached": 2}},
            {"type": "result", "stats": {"input_tokens": 30, "output_tokens": 15, "cached": 8}},
        ])
        assert agent.get_usage_metrics() == {
            "input_tokens": 40, "output_tokens": 20, "cached_input_tokens": 10
        }

    def test_skips_non_json_lines(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        agent.output_path.write_text(
            "not json\n"
            + json.dumps({"type": "result", "stats": {"input_tokens": 5, "output_tokens": 3, "cached": 0}})
            + "\n"
        )
        assert agent.get_usage_metrics()["input_tokens"] == 5


class TestGeminiCliAgentDoRun:
    def test_builds_correct_command(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            rc = agent._do_run("my task", tmp_path / "env")

        assert rc == 0
        cmd = mock_rs.call_args[0][0]
        assert cmd[0] == "gemini"
        assert [cmd[i + 1] for i, v in enumerate(cmd) if v == "-p"][0] == "my task"
        assert "stream-json" in cmd
        assert "yolo" in cmd
        assert "gemini-flash" in cmd

    def test_sets_google_api_key_in_env(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.dict(os.environ, {"GOOGLE_API_KEY": "test-key-123"}):
                agent._do_run("task", tmp_path / "env")
        assert mock_rs.call_args[0][2].get("GOOGLE_API_KEY") == "test-key-123"

    def test_falls_back_to_gemini_api_key(self, tmp_path):
        agent = GeminiCliAgent(logs_dir=tmp_path, model_name="gemini-flash")
        with patch.object(agent, "_run_subprocess", return_value=0) as mock_rs:
            with patch.dict(os.environ, {"GEMINI_API_KEY": "gemini-key", "GOOGLE_API_KEY": ""}):
                agent._do_run("task", tmp_path / "env")
        assert mock_rs.call_args[0][2].get("GOOGLE_API_KEY") == "gemini-key"
