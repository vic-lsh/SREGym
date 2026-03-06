"""
Codex agent implementation for SREGym.
Based on Harbor's Codex agent implementation for parity experiments.
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from clients.common.base_agent import BaseAgent, RunInterceptor

logger = logging.getLogger("all.codex.agent")


class CodexAgent(BaseAgent):
    _CLI_NAME = "codex"
    _OUTPUT_FILENAME = "codex.txt"

    @classmethod
    def _install(cls) -> None:
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "codex-cli"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Successfully installed Codex CLI")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to auto-install Codex CLI: {e}\n"
                "Please install it manually: pip install codex-cli"
            ) from e

    def __init__(
        self,
        logs_dir: Path,
        model_name: str,
        codex_home: Optional[Path] = None,
        interceptors: Optional[list[RunInterceptor]] = None,
    ):
        super().__init__(logs_dir=logs_dir, model_name=model_name, interceptors=interceptors)

        self.codex_home = Path(codex_home) if codex_home else self.logs_dir
        self.codex_home.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized Codex agent with model={model_name}")
        logger.info(f"Logs dir: {self.logs_dir}")
        logger.info(f"Codex home: {self.codex_home}")

    @property
    def trajectory_path(self) -> Path:
        return self.logs_dir / "trajectory.json"

    def get_usage_metrics(self) -> dict[str, int]:
        metrics = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}

        if not self.output_path.exists():
            logger.debug(f"Codex output file {self.output_path} does not exist")
            return metrics

        with open(self.output_path) as f:
            lines = f.readlines()

        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
                if isinstance(parsed, dict) and "usage" in parsed:
                    usage = parsed["usage"]
                    metrics["input_tokens"] = usage.get("input_tokens", 0)
                    metrics["cached_input_tokens"] = usage.get("cached_input_tokens", 0)
                    metrics["output_tokens"] = usage.get("output_tokens", 0)
                    logger.info(f"Extracted usage metrics: {metrics}")
                    return metrics
            except json.JSONDecodeError:
                continue

        return metrics

    def _setup_auth(self) -> None:
        auth_file = self.codex_home / "auth.json"
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            logger.warning("OPENAI_API_KEY not set in environment")
        with open(auth_file, "w") as f:
            json.dump({"OPENAI_API_KEY": api_key}, f)
        logger.info(f"Created auth file at {auth_file}")

    def _cleanup_auth(self) -> None:
        auth_file = self.codex_home / "auth.json"
        if auth_file.exists():
            auth_file.unlink()
            logger.info(f"Removed auth file at {auth_file}")

    def _do_run(self, instruction: str, exp_env_dir: Path) -> int:
        model = self.model_name.split("/")[-1]
        logger.info(f"Running Codex with model: {model}")

        self._setup_auth()
        try:
            env = os.environ.copy()
            env["CODEX_HOME"] = str(self.codex_home)

            command = [
                "codex", "exec",
                "--dangerously-bypass-approvals-and-sandbox",
                "--skip-git-repo-check",
                "--model", model,
                "--json",
                "--enable", "unified_exec",
                "-c", "model_reasoning_effort=high",
                "--",
                instruction,
            ]

            return self._run_subprocess(command, exp_env_dir, env)
        except Exception as e:
            logger.error(f"Error running Codex: {e}")
            raise
        finally:
            self._cleanup_auth()
