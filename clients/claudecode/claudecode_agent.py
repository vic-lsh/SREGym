"""
Claude Code agent implementation for SREGym.
Based on Harbor's Claude Code agent implementation for parity experiments.
"""

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from clients.common.base_agent import BaseAgent, RunInterceptor

logger = logging.getLogger("all.claudecode.agent")


class ClaudeCodeAgent(BaseAgent):
    _CLI_NAME = "claude"
    _OUTPUT_FILENAME = "claude-code.txt"

    ALLOWED_TOOLS = [
        "Bash", "Edit", "Write", "Read", "Glob", "Grep", "LS",
        "WebFetch", "NotebookEdit", "NotebookRead", "TodoRead", "TodoWrite",
        "Agent", "Skill", "SlashCommand", "Task", "WebSearch",
    ]

    @classmethod
    def _install(cls) -> None:
        try:
            subprocess.check_call(
                ["npm", "install", "-g", "@anthropic-ai/claude-code"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Successfully installed Claude Code CLI")
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "npm is not installed. Please install Node.js and npm first.\n"
                "Then run: npm install -g @anthropic-ai/claude-code"
            ) from e

    def __init__(
        self,
        logs_dir: Path,
        model_name: str,
        sessions_dir: Optional[Path] = None,
        interceptors: Optional[list[RunInterceptor]] = None,
    ):
        super().__init__(logs_dir=logs_dir, model_name=model_name, interceptors=interceptors)

        self.sessions_dir = Path(sessions_dir) if sessions_dir else self.logs_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized Claude Code agent with model={model_name}")
        logger.info(f"Logs dir: {self.logs_dir}")
        logger.info(f"Sessions dir: {self.sessions_dir}")

    @property
    def trajectory_path(self) -> Path:
        return self.logs_dir / "trajectory.json"

    def _get_session_dir(self) -> Path | None:
        """Identify the Claude session directory containing the primary JSONL log."""
        project_root = self.sessions_dir / "projects"
        if not project_root.exists():
            return None

        candidate_files = list(project_root.glob("**/*.jsonl"))
        if not candidate_files:
            return None

        candidate_dirs = sorted({f.parent for f in candidate_files if f.parent.is_dir()})
        if not candidate_dirs:
            return None

        if len(candidate_dirs) == 1:
            return candidate_dirs[0]

        logger.warning("Multiple Claude Code session directories found; could not identify the correct one")
        return None

    def get_usage_metrics(self) -> dict[str, int]:
        metrics = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}

        session_dir = self._get_session_dir()
        if not session_dir:
            logger.debug("No Claude Code session directory found")
            return metrics

        session_files = list(session_dir.glob("*.jsonl"))
        if not session_files:
            logger.debug(f"No session files found in {session_dir}")
            return metrics

        for session_file in session_files:
            with open(session_file, "r") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        event = json.loads(stripped)
                        usage = event.get("message", {}).get("usage") if isinstance(event.get("message"), dict) else None
                        if isinstance(usage, dict):
                            metrics["input_tokens"] += usage.get("input_tokens", 0)
                            metrics["cached_input_tokens"] += usage.get("cache_read_input_tokens", 0)
                            metrics["output_tokens"] += usage.get("output_tokens", 0)
                    except json.JSONDecodeError:
                        continue

        logger.info(f"Extracted usage metrics: {metrics}")
        return metrics

    def _setup_sessions_structure(self) -> None:
        for sub in ("debug", "projects/-app", "shell-snapshots", "statsig", "todos"):
            (self.sessions_dir / sub).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created session directory structure at {self.sessions_dir}")

    def _do_run(self, instruction: str, exp_env_dir: Path) -> int:
        model = self.model_name.split("/")[-1]

        invalid_patterns = ["bedrock", "litellm", "azure", "openai", "watsonx", "gemini"]
        if any(p in model.lower() for p in invalid_patterns):
            logger.warning(f"Model '{model}' appears non-Anthropic. Defaulting to 'sonnet'.")
            model = "sonnet"

        logger.info(f"Running Claude Code with model: {model}")

        self._setup_sessions_structure()

        env = os.environ.copy()
        env["CLAUDE_CONFIG_DIR"] = str(self.sessions_dir)
        env["FORCE_AUTO_BACKGROUND_TASKS"] = "1"
        env["ENABLE_BACKGROUND_TASKS"] = "1"

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        oauth_token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")

        if not api_key and not oauth_token:
            credentials_path = Path.home() / ".claude" / ".credentials.json"
            try:
                with open(credentials_path) as f:
                    creds = json.load(f)
                oauth_token = creds.get("claudeAiOauth", {}).get("accessToken", "")
                if oauth_token:
                    logger.info("Using local Claude Code credentials from %s", credentials_path)
            except (FileNotFoundError, json.JSONDecodeError, KeyError):
                pass

        if not api_key and not oauth_token:
            logger.error("=" * 80)
            logger.error("ERROR: No Anthropic API authentication found")
            logger.error("Please set ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN")
            logger.error("=" * 80)
            return 1

        if api_key:
            env["ANTHROPIC_API_KEY"] = api_key
        if oauth_token:
            env["CLAUDE_CODE_OAUTH_TOKEN"] = oauth_token

        env["ANTHROPIC_MODEL"] = model

        if "MAX_THINKING_TOKENS" in os.environ:
            env["MAX_THINKING_TOKENS"] = os.environ["MAX_THINKING_TOKENS"]

        command = [
            "claude",
            "--verbose",
            "--output-format", "stream-json",
            "-p", instruction,
            "--allowedTools",
        ] + self.ALLOWED_TOOLS

        try:
            return self._run_subprocess(command, exp_env_dir, env)
        except Exception as e:
            logger.error(f"Error running Claude Code: {e}")
            raise
