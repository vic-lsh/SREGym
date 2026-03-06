"Gemini CLI agent implementation for SREGym."

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

from clients.common.base_agent import BaseAgent, RunInterceptor

logger = logging.getLogger("all.gemini_cli.agent")


class GeminiCliAgent(BaseAgent):
    _CLI_NAME = "gemini"
    _OUTPUT_FILENAME = "gemini-cli.txt"

    @classmethod
    def _install(cls) -> None:
        try:
            subprocess.check_call(
                ["npm", "install", "-g", "@google/gemini-cli"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Successfully installed Gemini CLI")
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "npm is not installed. Please install Node.js and npm first.\n"
                "Then run: npm install -g @google/gemini-cli"
            ) from e

    def __init__(
        self,
        logs_dir: Path,
        model_name: str,
        sessions_dir: Optional[Path] = None,
        interceptors: Optional[list[RunInterceptor]] = None,
    ):
        super().__init__(logs_dir=logs_dir, model_name=model_name, interceptors=interceptors)

        self.model_name = model_name.removeprefix("vertex-ai-")
        self.sessions_dir = Path(sessions_dir) if sessions_dir else self.logs_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized Gemini CLI agent with model={self.model_name}")
        logger.info(f"Logs dir: {self.logs_dir}")
        logger.info(f"Sessions dir: {self.sessions_dir}")

    def get_usage_metrics(self) -> dict[str, int]:
        metrics = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}

        if not self.output_path.exists():
            logger.debug(f"Gemini output file {self.output_path} does not exist")
            return metrics

        with open(self.output_path, "r") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    event = json.loads(stripped)
                    if event.get("type") == "result":
                        stats = event.get("stats")
                        if isinstance(stats, dict):
                            metrics["input_tokens"] += stats.get("input_tokens", 0)
                            metrics["output_tokens"] += stats.get("output_tokens", 0)
                            metrics["cached_input_tokens"] += stats.get("cached", 0)
                except json.JSONDecodeError:
                    continue

        logger.info(f"Extracted usage metrics: {metrics}")
        return metrics

    def _do_run(self, instruction: str, exp_env_dir: Path) -> int:
        model = self.model_name
        logger.info(f"Running Gemini CLI with model: {model}")

        env = os.environ.copy()
        api_key = os.environ.get("GOOGLE_API_KEY", "") or os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            logger.warning("No Google/Gemini API key found (GOOGLE_API_KEY or GEMINI_API_KEY)")
        else:
            env["GOOGLE_API_KEY"] = api_key

        command = [
            "gemini",
            "-p", instruction,
            "--output-format", "stream-json",
            "--approval-mode", "yolo",
            "-m", model,
        ]

        try:
            return self._run_subprocess(command, exp_env_dir, env)
        except Exception as e:
            logger.error(f"Error running Gemini CLI: {e}")
            raise
