from __future__ import annotations

import os

from sregym.agent_launcher import AgentLauncher


class TestAgentLauncher:
    def test_cleanup_agent_cleans_exp_env_even_without_tracked_process(self, tmp_path):
        exp_env = tmp_path / "exp_env_1"
        exp_env.mkdir()
        stale_file = exp_env / "diagnosis_session_state.md"
        stale_file.write_text("stale content")

        launcher = AgentLauncher()

        old_env = os.environ.get("SREGYM_EXP_ENV")
        os.environ["SREGYM_EXP_ENV"] = str(exp_env)
        try:
            launcher.cleanup_agent("crucible")
        finally:
            if old_env is None:
                os.environ.pop("SREGYM_EXP_ENV", None)
            else:
                os.environ["SREGYM_EXP_ENV"] = old_env

        assert exp_env.is_dir()
        assert not stale_file.exists()
        assert list(exp_env.iterdir()) == []
