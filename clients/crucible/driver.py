"""Crucible agent driver — entry point for the judge-agent benchmark client."""

import argparse
import asyncio
import logging
import os
import shutil
from pathlib import Path

from clients.common.driver_utils import (
    get_app_info,
    get_planned_stages,
    get_problem_id,
    save_results,
    wait_for_ready_stage,
)
from clients.crucible import orchestrator
from clients.common.summarize import SUMMARY_FILENAME
from clients.crucible.summary import CrucibleLTSummarizer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crucible judge-agent benchmark client")
    parser.add_argument(
        "--logs-dir",
        type=str,
        default=None,
        help="Directory for result JSON output (e.g. token usage)",
    )
    parser.add_argument(
        "--summary-dir",
        type=str,
        default=None,
        help="Directory for long_term_summary.txt (enables long-term summary mode when set)",
    )
    parser.add_argument(
        "--summary-model",
        type=str,
        default=None,
        help="Model ID to use for long-term summarization (defaults to MODEL_ID env var)",
    )
    parser.add_argument(
        "--no-inject-summary",
        action="store_true",
        default=False,
        help="Update the long-term summary after each run but do not inject it before the run",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    logger.info("Crucible driver starting...")

    # Block until the conductor reaches a submission-ready stage
    stage = wait_for_ready_stage()
    logger.info(f"Conductor ready at stage: {stage!r}")

    app_info = get_app_info()
    problem_id = get_problem_id()
    planned_stages = get_planned_stages()

    exp_env = os.getenv("SREGYM_EXP_ENV", ".")
    shared_file = Path(exp_env) / "judged_session_state.md"

    # Long-term summary mode: enabled when --summary-dir is provided.
    # Before the run, copy long_term_summary.txt into the agent's exp_env dir so
    # the agent can read it by path. After the run, update the long-term summary
    # from the session markdown.
    lt_summarizer: CrucibleLTSummarizer | None = None
    lt_summary_file: Path | None = None

    if args.summary_dir:
        model_id = args.summary_model or os.environ.get("MODEL_ID", "gpt-4o")
        lt_summarizer = CrucibleLTSummarizer(
            shared_file=shared_file,
            summary_dir=Path(args.summary_dir),
            model_id=model_id,
        )
        if not args.no_inject_summary:
            dest = Path(exp_env) / SUMMARY_FILENAME
            try:
                shutil.copy2(lt_summarizer.summary_path, dest)
                lt_summary_file = dest
                logger.info(f"Long-term summary: copied prior knowledge to {dest}")
            except FileNotFoundError:
                logger.info("Long-term summary: no prior summary found; starting fresh.")

    usage_metrics = await orchestrator.run(
        app_info=app_info,
        problem_id=problem_id,
        shared_file=shared_file,
        planned_stages=planned_stages,
        lt_summary_file=lt_summary_file,
    )

    if args.logs_dir:
        logs_dir = Path(args.logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        save_results(logs_dir, problem_id, 0, usage_metrics, prefix="crucible")
        logger.info(f"Usage metrics: {usage_metrics}")


    env_log_file = os.environ.get("SREGYM_LOG_FILE")
    if env_log_file and shared_file.exists():
        dest = Path(env_log_file).with_suffix(".md")
        shutil.copy2(shared_file, dest)
        logger.info(f"Saved session markdown to {dest}")

    if lt_summarizer is not None:
        logger.info("Long-term summary: updating from completed session.")
        lt_summarizer.run()

    logger.info("Crucible driver complete.")


if __name__ == "__main__":
    asyncio.run(main())
