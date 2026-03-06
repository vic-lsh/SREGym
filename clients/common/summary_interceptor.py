"""
Pluggable summary interceptor: injects past findings before a run and
updates the long-term summary after.
"""

import logging
import shutil
from pathlib import Path

from clients.common.base_agent import AfterRunContext, BeforeRunContext, RunInterceptor
from clients.common.summarize import ResultSummarizer

logger = logging.getLogger("all.common.summary_interceptor")


class SummaryInterceptor(RunInterceptor):
    """
    Interceptor that maintains a long-term summary across agent runs.

    Before each run, if a summary file exists and injection is enabled, the
    summary is copied into the agent's working directory and a note is appended
    to the instruction so the agent knows to read it.

    After each run, ResultSummarizer is called to update the summary file with
    findings from the completed run.

    Args:
        summary_dir: Directory where ``long_term_summary.txt`` is read from and
            written to.
        model_id: Model identifier passed to ResultSummarizer for generating
            the updated summary.
        inject: If False, the summary is updated after each run but never
            injected into the instruction before the run.
    """

    _SUMMARY_FILENAME = "long_term_summary.txt"

    def __init__(self, summary_dir: Path, model_id: str, inject: bool = True):
        self.summary_dir = Path(summary_dir)
        self.summary_dir.mkdir(parents=True, exist_ok=True)
        self.model_id = model_id
        self.inject = inject

    @property
    def summary_path(self) -> Path:
        return self.summary_dir / self._SUMMARY_FILENAME

    def before_run(self, ctx: BeforeRunContext) -> None:
        if not self.inject or not self.summary_path.exists():
            return
        try:
            dest = ctx.exp_env_dir / self._SUMMARY_FILENAME
            shutil.copy2(self.summary_path, dest)
            ctx.instruction += (
                f"\n\nIMPORTANT: A summary of findings from previous runs is available at: "
                f"{self._SUMMARY_FILENAME}\n"
                "Read it to avoid repeating mistakes or to speed up diagnosis.\n"
            )
            logger.info("Copied long-term summary into agent cwd and appended reference to instruction.")
        except Exception as e:
            logger.warning(f"Failed to copy summary into agent cwd: {e}")

    def after_run(self, ctx: AfterRunContext) -> None:
        agent = ctx.agent
        ResultSummarizer(
            logs_dir=agent.logs_dir,
            model_id=self.model_id,
            output_filename=agent._OUTPUT_FILENAME,
            summary_dir=self.summary_dir,
        ).run()
