"""
Shared utility functions for SREGym agent drivers.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger("all.common.driver_utils")


def get_api_base_url() -> str:
    host = os.getenv("API_HOSTNAME", "localhost")
    port = os.getenv("API_PORT", "8000")
    return f"http://{host}:{port}"


def get_app_info() -> dict:
    api_url = f"{get_api_base_url()}/get_app"
    logger.info(f"Fetching app info from {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        app_info = response.json()
        logger.info(f"App info: {app_info}")
        return app_info
    except Exception as e:
        logger.error(f"Failed to get app info: {e}")
        raise


def get_problem_id() -> str:
    api_url = f"{get_api_base_url()}/get_problem"
    logger.info(f"Fetching problem ID from {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        problem_data = response.json()
        problem_id = problem_data.get("problem_id")
        logger.info(f"Problem ID: {problem_id}")
        return problem_id
    except Exception as e:
        logger.error(f"Failed to get problem ID: {e}")
        raise


def get_planned_stages() -> list[str]:
    api_url = f"{get_api_base_url()}/stages"
    logger.info(f"Fetching planned stages from {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        stages = response.json().get("stages", [])
        logger.info(f"Planned stages: {stages}")
        return stages
    except Exception as e:
        logger.error(f"Failed to get planned stages: {e}")
        raise


def wait_for_ready_stage(timeout: int = 300) -> str:
    """
    Poll the conductor until it reaches a submission-ready stage.

    Returns the current stage name ("diagnosis" or "mitigation").
    Raises TimeoutError if the timeout expires.
    """
    api_url = f"{get_api_base_url()}/status"
    allowed_stages = {"diagnosis", "mitigation"}
    start_time = time.time()

    logger.info("Waiting for conductor to reach submission-ready stage...")

    while time.time() - start_time < timeout:
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            stage = response.json().get("stage")
            if stage in allowed_stages:
                logger.info(f"Conductor ready at stage: {stage}")
                return stage
            logger.debug(f"Current stage: {stage}, waiting for {allowed_stages}...")
        except Exception as e:
            logger.debug(f"Error checking status: {e}, retrying...")
        time.sleep(1)

    raise TimeoutError(f"Conductor did not reach ready stage within {timeout} seconds")


def build_instruction(
    app_info: dict,
    problem_id: str,
    planned_stages: list[str] | None = None,
    extra_preamble: str = "",
) -> str:
    """
    Build the task instruction string.

    Args:
        app_info: Application info from conductor.
        problem_id: Problem identifier.
        planned_stages: If provided, controls whether a mitigation task is included.
                        None means both diagnosis + mitigation (default behaviour).
        extra_preamble: Additional text inserted after the app description (e.g. Codex automation notice).
    """
    app_name = app_info.get("app_name", "unknown")
    namespace = app_info.get("namespace", "default")
    descriptions = app_info.get("descriptions", "")

    if planned_stages is None:
        has_mitigation = True
    else:
        has_mitigation = "mitigation" in planned_stages

    task_count = "TWO" if has_mitigation else "ONE"
    task_verb = "diagnosing and fixing" if has_mitigation else "diagnosing"
    base_url = get_api_base_url()

    preamble_block = f"\n{extra_preamble}\n" if extra_preamble else ""

    instruction = f"""You are an SRE agent tasked with {task_verb} issues in a Kubernetes application.

Application: {app_name}
Namespace: {namespace}

{descriptions}
{preamble_block}
WORKFLOW: You will perform {task_count} task{"s" if has_mitigation else ""} in sequence:

TASK 1: DIAGNOSIS
- Investigate the application to detect any anomalies or issues
- Analyze metrics, logs, and traces
- When ready, submit a natural language description of the issue you found
"""

    if has_mitigation:
        instruction += """
TASK 2: MITIGATION
- Identify the root cause of the issue
- Implement a fix to resolve the problem
- When the fix is applied, submit to trigger validation
"""

    instruction += f"""
HOW TO SUBMIT:

The submission endpoint is: {base_url}/submit

For DIAGNOSIS stage:
- Submit with a natural language description of the issue
- Example: POST {base_url}/submit with JSON: {{"solution": "The frontend service is crashing due to missing environment variable"}}
"""

    if has_mitigation:
        instruction += f"""
For MITIGATION stage:
- Submit with an EMPTY STRING after you have applied the fix
- POST {base_url}/submit with JSON: {{"solution": ""}}
"""

    instruction += f"""
Important:
- You have access to kubectl commands to inspect and modify resources in namespace '{namespace}'
- You can query metrics and traces through the available observability tools
- The conductor API is available at {base_url}
"""

    logger.info(f"Built instruction:\n{instruction}")
    return instruction


def save_results(
    logs_dir: Path,
    problem_id: str,
    return_code: int,
    usage_metrics: dict,
    prefix: str,
) -> None:
    """Save run results to a JSON file named {prefix}_results_{problem_id}_{timestamp}.json."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = logs_dir / f"{prefix}_results_{problem_id}_{timestamp}.json"

    results = {
        "problem_id": problem_id,
        "timestamp": timestamp,
        "return_code": return_code,
        "success": return_code == 0,
        "usage_metrics": usage_metrics,
    }

    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved results to {results_file}")
