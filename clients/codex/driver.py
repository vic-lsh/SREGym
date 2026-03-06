"""
Codex agent driver for SREGym.
Entry point for running Codex agent on SREGym tasks.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add SREGym root to path
sregym_root = Path(__file__).resolve().parents[2]
if str(sregym_root) not in sys.path:
    sys.path.insert(0, str(sregym_root))

from logger import init_logger

init_logger()

from clients.codex.codex_agent import CodexAgent
from clients.common.driver_utils import (
    build_instruction,
    get_app_info,
    get_planned_stages,
    get_problem_id,
    save_results,
    wait_for_ready_stage,
)

logger = logging.getLogger("all.codex.driver")

_CODEX_PREAMBLE = (
    "CRITICAL: You are running in an AUTOMATED environment. "
    "Work autonomously and make all decisions yourself. "
    "DO NOT ask for user confirmation or approval. "
    "Proceed with the best solution based on your analysis."
)


def main():
    parser = argparse.ArgumentParser(description="Run Codex agent on SREGym tasks")
    parser.add_argument(
        "--model",
        type=str,
        default=os.getenv("MODEL_ID", "claude-sonnet-4-5"),
        help="Model to use for Codex (default: from MODEL_ID env var or claude-sonnet-4-5)",
    )
    parser.add_argument(
        "--logs-dir",
        type=str,
        default="./logs/codex",
        help="Directory to store logs (default: ./logs/codex)",
    )
    parser.add_argument(
        "--codex-home",
        type=str,
        default=None,
        help="Codex home directory (default: same as logs-dir)",
    )
    parser.add_argument(
        "--no-auto-install",
        action="store_true",
        help="Disable auto-installation of Codex CLI if not found",
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Codex agent for SREGym")
    logger.info(f"Model: {args.model}")
    logger.info(f"Logs directory: {args.logs_dir}")
    logger.info("=" * 80)

    try:
        CodexAgent.ensure_installed(auto_install=not args.no_auto_install)
    except RuntimeError as e:
        logger.error(f"Codex CLI installation check failed: {e}")
        sys.exit(1)

    try:
        wait_for_ready_stage(timeout=300)
    except TimeoutError as e:
        logger.error(f"Timeout waiting for conductor: {e}")
        sys.exit(1)

    try:
        app_info = get_app_info()
        problem_id = get_problem_id()
        planned_stages = get_planned_stages()
    except Exception as e:
        logger.error(f"Failed to get problem information: {e}")
        sys.exit(1)

    instruction = build_instruction(
        app_info, problem_id, planned_stages, extra_preamble=_CODEX_PREAMBLE
    )

    logs_dir = Path(args.logs_dir)
    codex_home = Path(args.codex_home) if args.codex_home else None

    agent = CodexAgent(
        logs_dir=logs_dir,
        model_name=args.model,
        codex_home=codex_home,
    )

    logger.info("Starting Codex execution...")
    return_code = agent.run(instruction)
    usage_metrics = agent.get_usage_metrics()
    save_results(logs_dir, problem_id, return_code, usage_metrics, prefix="codex")

    logger.info("=" * 80)
    logger.info("Codex execution completed")
    logger.info(f"Return code: {return_code}")
    logger.info(f"Usage metrics: {usage_metrics}")
    logger.info("=" * 80)

    sys.exit(return_code)


if __name__ == "__main__":
    main()
