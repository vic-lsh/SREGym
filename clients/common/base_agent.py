"""
Abstract base class for SREGym CLI agent wrappers.
"""

import logging
import os
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("all.common.base_agent")


@dataclass
class BeforeRunContext:
    instruction: str  # mutable — interceptors may update this in-place
    agent: "BaseAgent"
    exp_env_dir: Path


@dataclass
class AfterRunContext:
    return_code: int
    agent: "BaseAgent"


class RunInterceptor:
    """Protocol-style base for run interceptors. Override before_run / after_run."""

    def before_run(self, ctx: BeforeRunContext) -> None:
        pass

    def after_run(self, ctx: AfterRunContext) -> None:
        pass


class BaseAgent(ABC):
    _CLI_NAME: str = ""       # e.g. "gemini", "claude", "codex"
    _OUTPUT_FILENAME: str = ""  # e.g. "gemini-cli.txt"

    # ------------------------------------------------------------------
    # Installation helpers
    # ------------------------------------------------------------------

    @classmethod
    def check_installation(cls) -> bool:
        return shutil.which(cls._CLI_NAME) is not None

    @classmethod
    def ensure_installed(cls, auto_install: bool = True) -> None:
        if cls.check_installation():
            logger.info(f"{cls._CLI_NAME} is already installed")
            return

        logger.warning(f"{cls._CLI_NAME} not found in PATH")

        if not auto_install:
            raise RuntimeError(
                f"{cls._CLI_NAME} is not installed. Run the agent's _install() manually."
            )

        logger.info(f"Attempting to install {cls._CLI_NAME}...")
        try:
            cls._install()
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError(f"Failed to auto-install {cls._CLI_NAME}: {e}") from e

        if not cls.check_installation():
            raise RuntimeError(
                f"{cls._CLI_NAME} installation appeared to succeed but command is still not available"
            )

    @classmethod
    @abstractmethod
    def _install(cls) -> None:
        """Install the CLI tool. Raise on failure."""

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    def __init__(
        self,
        logs_dir: Path,
        model_name: str,
        interceptors: Optional[list[RunInterceptor]] = None,
    ):
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.model_name = model_name
        self.interceptors: list[RunInterceptor] = interceptors or []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def output_path(self) -> Path:
        return self.logs_dir / self._OUTPUT_FILENAME

    # ------------------------------------------------------------------
    # Shared subprocess runner
    # ------------------------------------------------------------------

    def _run_subprocess(self, command: list[str], exp_env_dir: Path, env: dict) -> int:
        logger.info(f"Executing command: {' '.join(command)}")
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
            for line in process.stdout:
                out_file.write(line)
                out_file.flush()
                print(line, end="", flush=True)
            process.wait()
        return process.returncode

    # ------------------------------------------------------------------
    # Template method: run()
    # ------------------------------------------------------------------

    def run(self, instruction: str) -> int:
        exp_env_dir = Path(os.getenv("SREGYM_EXP_ENV", "exp_env"))
        exp_env_dir.mkdir(exist_ok=True, parents=True)

        before_ctx = BeforeRunContext(instruction=instruction, agent=self, exp_env_dir=exp_env_dir)
        for interceptor in self.interceptors:
            interceptor.before_run(before_ctx)

        # Write (possibly mutated) instruction to file for ResultSummarizer
        try:
            (self.logs_dir / "instruction.txt").write_text(before_ctx.instruction)
        except Exception as e:
            logger.warning(f"Failed to save instruction.txt: {e}")

        rc = self._do_run(before_ctx.instruction, exp_env_dir)

        after_ctx = AfterRunContext(return_code=rc, agent=self)
        for interceptor in reversed(self.interceptors):
            interceptor.after_run(after_ctx)

        return rc

    # ------------------------------------------------------------------
    # Abstract per-agent hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _do_run(self, instruction: str, exp_env_dir: Path) -> int:
        """Build the CLI command and run it; return the process return code."""

    @abstractmethod
    def get_usage_metrics(self) -> dict[str, int]:
        """Parse agent output and return token usage dict."""
