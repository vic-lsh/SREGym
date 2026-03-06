"""
Gemini CLI agent driver for SREGym.
Entry point for running Gemini CLI agent on SREGym tasks.
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

from clients.common.driver_utils import (
    build_instruction,
    get_app_info,
    get_planned_stages,
    get_problem_id,
    save_results,
    wait_for_ready_stage,
)
from clients.common.summary_interceptor import SummaryInterceptor
from clients.gemini_cli.gemini_cli_agent import GeminiCliAgent

logger = logging.getLogger("all.gemini_cli.driver")


def main():
    parser = argparse.ArgumentParser(description="Run Gemini CLI agent on SREGym tasks")
    parser.add_argument(
        "--model",
        type=str,
        default=os.getenv("MODEL_ID", "gemini-2.0-flash"),
        help="Model to use for Gemini CLI (default: from MODEL_ID env var or gemini-2.0-flash)",
    )
    parser.add_argument(
        "--logs-dir",
        type=str,
        default="./logs/gemini_cli",
        help="Directory to store logs (default: ./logs/gemini_cli)",
    )
    parser.add_argument(
        "--sessions-dir",
        type=str,
        default=None,
        help="Gemini CLI sessions directory (default: logs-dir/sessions)",
    )
    parser.add_argument(
        "--summary-dir",
        type=str,
        default=None,
        help="Directory for long-term summary (default: logs-dir)",
    )
    parser.add_argument(
        "--no-auto-install",
        action="store_true",
        help="Disable auto-installation of Gemini CLI if not found",
    )
    parser.add_argument(
        "--enable-summary",
        action="store_true",
        help="Enable summarization of runs across iterations",
    )
    parser.add_argument(
        "--no-inject-summary",
        action="store_true",
        help="Build summaries but do not pass them to the agent",
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Gemini CLI agent for SREGym")
    logger.info(f"Model: {args.model}")
    logger.info(f"Logs directory: {args.logs_dir}")
    logger.info(f"Enable summary: {args.enable_summary}")
    logger.info(f"Inject summary: {not args.no_inject_summary}")
    logger.info("=" * 80)

    try:
        GeminiCliAgent.ensure_installed(auto_install=not args.no_auto_install)
    except RuntimeError as e:
        logger.error(f"Gemini CLI installation check failed: {e}")
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

    instruction = build_instruction(app_info, problem_id, planned_stages)

    logs_dir = Path(args.logs_dir)
    sessions_dir = Path(args.sessions_dir) if args.sessions_dir else None
    summary_dir = Path(args.summary_dir) if args.summary_dir else logs_dir

    interceptors = []
    if args.enable_summary:
        interceptors.append(
            SummaryInterceptor(
                summary_dir=summary_dir,
                model_id=args.model,
                inject=not args.no_inject_summary,
            )
        )

    agent = GeminiCliAgent(
        logs_dir=logs_dir,
        model_name=args.model,
        sessions_dir=sessions_dir,
        interceptors=interceptors,
    )

    logger.info("Starting Gemini CLI execution...")
    return_code = agent.run(instruction)
    usage_metrics = agent.get_usage_metrics()
    save_results(logs_dir, problem_id, return_code, usage_metrics, prefix="gemini_cli")

    logger.info("=" * 80)
    logger.info("Gemini CLI execution completed")
    logger.info(f"Return code: {return_code}")
    logger.info(f"Usage metrics: {usage_metrics}")
    logger.info("=" * 80)

    sys.exit(return_code)


if __name__ == "__main__":
    main()
