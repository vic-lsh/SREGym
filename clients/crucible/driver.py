"""Crucible agent driver — entry point for the judge-agent benchmark client."""

import asyncio
import logging
import os
import shutil
from pathlib import Path

from clients.common.driver_utils import (
    get_app_info,
    get_planned_stages,
    get_problem_id,
    wait_for_ready_stage,
)
from clients.crucible import orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Crucible driver starting...")

    # Block until the conductor reaches a submission-ready stage
    stage = wait_for_ready_stage()
    logger.info(f"Conductor ready at stage: {stage!r}")

    app_info = get_app_info()
    problem_id = get_problem_id()
    planned_stages = get_planned_stages()

    exp_env = os.getenv("SREGYM_EXP_ENV", ".")
    shared_file = Path(exp_env) / "judged_session_state.md"

    await orchestrator.run(
        app_info=app_info,
        problem_id=problem_id,
        shared_file=shared_file,
        planned_stages=planned_stages,
    )

    env_log_file = os.environ.get("SREGYM_LOG_FILE")
    if env_log_file and shared_file.exists():
        dest = Path(env_log_file).with_suffix(".md")
        shutil.copy2(shared_file, dest)
        logger.info(f"Saved session markdown to {dest}")

    logger.info("Crucible driver complete.")


if __name__ == "__main__":
    asyncio.run(main())
