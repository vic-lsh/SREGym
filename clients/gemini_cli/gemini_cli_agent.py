"Gemini CLI agent implementation for SREGym."

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("all.gemini_cli.agent")


class GeminiCliAgent:
    """
    The Gemini CLI agent uses Google's Gemini CLI tool to solve tasks.
    """

    _OUTPUT_FILENAME = "gemini-cli.txt"
    _SUMMARY_FILENAME = "long_term_summary.txt"

    @staticmethod
    def check_installation() -> bool:
        """
        Check if Gemini CLI is installed.

        Returns:
            True if gemini is available, False otherwise
        """
        return shutil.which("gemini") is not None

    @staticmethod
    def ensure_installed(auto_install: bool = True) -> None:
        """
        Ensure Gemini CLI is installed, optionally attempting installation.

        Args:
            auto_install: If True, attempt to install gemini if not found

        Raises:
            RuntimeError: If gemini is not installed and auto_install fails
        """
        if GeminiCliAgent.check_installation():
            logger.info("Gemini CLI is already installed")
            return

        logger.warning("Gemini CLI not found in PATH")

        if not auto_install:
            raise RuntimeError(
                "Gemini CLI is not installed. Please install it using:\n  npm install -g @google/gemini-cli\n"
            )

        # Attempt auto-installation
        logger.info("Attempting to install Gemini CLI via npm...")
        try:
            subprocess.check_call(
                ["npm", "install", "-g", "@google/gemini-cli"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Successfully installed Gemini CLI")

            # Verify installation
            if not GeminiCliAgent.check_installation():
                raise RuntimeError("Gemini CLI installation appeared to succeed but command is still not available")

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            error_msg = f"Failed to auto-install Gemini CLI: {e}\n"
            if isinstance(e, FileNotFoundError):
                error_msg += "npm is not installed. Please install Node.js and npm first.\n"
            error_msg += "Please install Gemini CLI manually using:\n  npm install -g @google/gemini-cli\n"
            raise RuntimeError(error_msg)

    def __init__(
        self,
        logs_dir: Path,
        model_name: str,
        sessions_dir: Optional[Path] = None,
        summary_dir: Optional[Path] = None,
        enable_summary: bool = False,
        inject_summary: bool = True,
    ):
        """
        Initialize the Gemini CLI agent.

        Args:
            logs_dir: Directory to store logs and output
            model_name: Model name to use (e.g., "gemini-2.0-flash")
            sessions_dir: Directory for Gemini sessions (defaults to logs_dir/sessions)
            summary_dir: Directory for long-term summary (defaults to logs_dir)
            enable_summary: If True, enable accumulation of summaries across runs
            inject_summary: If True, pass summary to agent in prompt (requires enable_summary)
        """
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self.model_name = model_name.removeprefix("vertex-ai-")
        self.sessions_dir = Path(sessions_dir) if sessions_dir else self.logs_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

        self.summary_dir = Path(summary_dir) if summary_dir else self.logs_dir
        self.summary_dir.mkdir(parents=True, exist_ok=True)

        self.enable_summary = enable_summary
        self.inject_summary = inject_summary

        logger.info(f"Initialized Gemini CLI agent with model={self.model_name}")
        logger.info(f"Logs dir: {self.logs_dir}")
        logger.info(f"Sessions dir: {self.sessions_dir}")
        logger.info(f"Summary dir: {self.summary_dir}")
        logger.info(f"Enable summary: {self.enable_summary}")
        logger.info(f"Inject summary: {self.inject_summary}")

    @property
    def output_path(self) -> Path:
        """Path to Gemini CLI output file."""
        return self.logs_dir / self._OUTPUT_FILENAME

    @property
    def summary_path(self) -> Path:
        """Path to the long-term summary file."""
        return self.summary_dir / self._SUMMARY_FILENAME

    def get_usage_metrics(self) -> dict[str, int]:
        """
        Extract usage metrics from Gemini CLI output file.

        Returns:
            Dictionary with keys: input_tokens, cached_input_tokens, output_tokens
        """
        metrics = {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
        }

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
                    # Check for "result" event which contains "stats"
                    # {"type":"result", ..., "stats":{"total_tokens":..., "input_tokens":..., "output_tokens":..., "cached":...}}
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

    def _get_response_text(self) -> str:
        """Extract response text from Gemini CLI output file."""
        text = ""
        if not self.output_path.exists():
            return text

        with open(self.output_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    # Assuming standard gemini cli output structure for stream-json
                    # 'content' type usually contains text chunks
                    if event.get("type") == "content":
                        text += event.get("content", "")
                except json.JSONDecodeError:
                    pass
        return text

    def run(self, instruction: str) -> int:
        """
        Run the Gemini CLI agent with the given instruction.

        Args:
            instruction: The task instruction to pass to Gemini CLI

        Returns:
            Return code from Gemini CLI execution (0 for success)
        """
        model = self.model_name

        # Ensure exp_env directory exists (agent's cwd)
        exp_env_dir = Path(os.getenv("SREGYM_EXP_ENV", "exp_env"))
        exp_env_dir.mkdir(exist_ok=True, parents=True)

        # If summary is enabled, injection is on, and file exists, copy it into agent's cwd and reference by path
        if self.enable_summary and self.inject_summary and self.summary_path.exists():
            try:
                summary_in_cwd = exp_env_dir / self._SUMMARY_FILENAME
                shutil.copy2(self.summary_path, summary_in_cwd)
                instruction += f"\n\nIMPORTANT: A summary of findings from previous runs is available at: {self._SUMMARY_FILENAME}\nRead it to avoid repeating mistakes or to speed up diagnosis.\n"
                logger.info("Copied existing long-term summary into agent cwd and referenced by path.")
            except Exception as e:
                logger.warning(f"Failed to copy existing summary: {e}")

        logger.info(f"Running Gemini CLI with instruction: {instruction}")
        logger.info(f"Using model: {model}")

        # Build environment variables
        env = os.environ.copy()

        # Set API key if available
        api_key = os.environ.get("GOOGLE_API_KEY", "") or os.environ.get("GEMINI_API_KEY", "")

        if not api_key:
            logger.warning("No Google/Gemini API key found in environment variables (GOOGLE_API_KEY or GEMINI_API_KEY)")
            # We don't return here, letting the CLI fail or prompt if it handles it.
            # But normally we should probably warn louder.
        else:
            env["GOOGLE_API_KEY"] = api_key

        # Build Gemini CLI command
        # gemini -p "prompt" --output-format stream-json --approval-mode yolo -m model
        command = [
            "gemini",
            "-p",
            instruction,
            "--output-format",
            "stream-json",
            "--approval-mode",
            "yolo",
            "-m",
            model,
        ]

        logger.info(f"Executing command: {' '.join(command)}")

        try:
            # Run Gemini CLI and capture output
            with open(self.output_path, "w") as out_file:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                    text=True,
                    bufsize=1,
                    cwd=exp_env_dir,
                )

                # Stream output to both file and logger
                for line in process.stdout:
                    out_file.write(line)
                    out_file.flush()
                    # Also log to console (strip to avoid double newlines)
                    print(line, end="", flush=True)

                process.wait()

            logger.info(f"Gemini CLI finished with return code: {process.returncode}")

            # Summarization is now handled externally by main.py / summarize_results.py

            return process.returncode

        except Exception as e:
            logger.error(f"Error running Gemini CLI: {e}")
            raise
