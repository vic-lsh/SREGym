"""Tests for the submit MCP server's autonomous-mode tools.

The MCP server is launched in-process on each worker before the cli_agent
subprocess starts. It reads ``SREGYM_AUTONOMOUS_SUBMIT`` at module load to
decide whether to register the single ``submit`` tool (default) or the
per-stage ``submit_diagnosis`` / ``submit_mitigation`` tools. The wrong
tool set would mislead the agent — these tests lock in both paths.
"""

from __future__ import annotations

import importlib
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


def _reload_submit_server(env: dict[str, str] | None = None):
    """Reload the submit_server module with a controlled environment.

    Tools are registered at import time via ``@mcp.tool`` decorators, so
    toggling the env var only takes effect on a fresh import.
    """
    with patch.dict(os.environ, env or {}, clear=False):
        sys.modules.pop("mcp_server.submit_server", None)
        module = importlib.import_module("mcp_server.submit_server")
    return module


@pytest.fixture
def _clear_env(monkeypatch):
    monkeypatch.delenv("SREGYM_AUTONOMOUS_SUBMIT", raising=False)
    sys.modules.pop("mcp_server.submit_server", None)
    yield
    sys.modules.pop("mcp_server.submit_server", None)


def _registered_tools(module) -> set[str]:
    """Extract tool names from a FastMCP server regardless of fastmcp version.

    FastMCP exposes its tool registry slightly differently across versions;
    try the public helpers first and fall back to the private dict.
    """
    mcp = module.mcp
    for attr in ("_tool_manager", "_tools"):
        registry = getattr(mcp, attr, None)
        if registry is None:
            continue
        inner = getattr(registry, "_tools", None) or getattr(registry, "tools", None)
        if isinstance(inner, dict):
            return set(inner.keys())
    # Last resort: fastmcp >= 2 exposes list_tools() as async; call sync helper.
    list_tools = getattr(mcp, "list_tools", None)
    if callable(list_tools):
        try:
            import asyncio

            tools = asyncio.run(list_tools())
            return {t.name for t in tools}
        except Exception:
            pass
    raise RuntimeError("Could not enumerate registered MCP tools")


class TestToolRegistration:
    def test_default_mode_registers_submit(self, _clear_env):
        module = _reload_submit_server({})
        tools = _registered_tools(module)
        assert "submit" in tools
        assert "submit_diagnosis" not in tools
        assert "submit_mitigation" not in tools

    def test_autonomous_mode_registers_per_stage_tools(self, _clear_env):
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        tools = _registered_tools(module)
        assert "submit_diagnosis" in tools
        assert "submit_mitigation" in tools
        # The stage-routing `submit` tool must not coexist — it would
        # confuse the agent about which tool to call.
        assert "submit" not in tools


class TestSubmitDiagnosisTool:
    def test_posts_to_submit_stage_with_diagnosis(self, _clear_env):
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "recorded"}'
        with patch.object(module.requests, "post", return_value=mock_resp) as mock_post:
            result = module.submit_diagnosis.fn("root-cause X")

        assert mock_post.call_count == 1
        kwargs = mock_post.call_args.kwargs
        payload = kwargs.get("json") or mock_post.call_args.args[1]
        assert payload == {"solution": "root-cause X", "stage": "diagnosis"}
        url = mock_post.call_args.args[0] if mock_post.call_args.args else kwargs["url"]
        assert url.endswith("/submit_stage")
        assert result == {"status": "recorded"}

    def test_list_solution_passes_through(self, _clear_env):
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "recorded"}'
        with patch.object(module.requests, "post", return_value=mock_resp) as mock_post:
            module.submit_diagnosis.fn(["a", "b"])

        payload = mock_post.call_args.kwargs["json"]
        assert payload == {"solution": ["a", "b"], "stage": "diagnosis"}

    def test_response_does_not_leak_grading(self, _clear_env):
        """Even if the conductor accidentally returned grading fields in
        the HTTP body, the tool must only forward the neutral ack — the
        agent must never see a verdict signal in autonomous mode."""
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "recorded", "oops": "accepted"}'
        with patch.object(module.requests, "post", return_value=mock_resp):
            result = module.submit_diagnosis.fn("x")
        assert result == {"status": "recorded"}
        assert "accepted" not in str(result)

    def test_http_error_does_not_leak_oracle_details(self, _clear_env):
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "Bad stage"
        with patch.object(module.requests, "post", return_value=mock_resp):
            result = module.submit_diagnosis.fn("x")
        # Non-200 surfaces as status != "recorded" so the agent can at
        # least tell "the submission was not recorded" and retry the
        # call, but we still keep the body short.
        assert result["status"] != "recorded"


class TestSubmitMitigationTool:
    def test_posts_to_submit_stage_with_mitigation(self, _clear_env):
        module = _reload_submit_server({"SREGYM_AUTONOMOUS_SUBMIT": "1"})
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "recorded"}'
        with patch.object(module.requests, "post", return_value=mock_resp) as mock_post:
            result = module.submit_mitigation.fn("restarted deployment")

        payload = mock_post.call_args.kwargs["json"]
        assert payload == {"solution": "restarted deployment", "stage": "mitigation"}
        assert result == {"status": "recorded"}
