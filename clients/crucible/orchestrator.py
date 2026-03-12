import logging
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from langchain_core.messages import HumanMessage, SystemMessage

from clients.common.driver_utils import wait_for_ready_stage
from clients.crucible.agents.agent import CrucibleAgent
from clients.crucible.tools.bash_tool import exec_bash, exec_bash_readonly
from clients.crucible.tools.file_tools import read_file, str_replace_file, write_file
from clients.crucible.tools.judge_tools import make_approve_and_submit, make_reject_with_feedback
from clients.crucible.tools.sre_complete_tool import (
    make_mark_hypothesis_complete,
    make_mark_mitigation_complete,
)
from llm_backend.init_backend import get_llm_backend_for_tools

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent / "configs"
_PROMPTS_DIR = _CONFIG_DIR / "prompts"

_jinja_env = Environment(
    loader=FileSystemLoader(str(_PROMPTS_DIR)),
    undefined=StrictUndefined,
    keep_trailing_newline=True,
)


def _render(template_name: str, **kwargs: object) -> str:
    """Render a Jinja2 prompt template from configs/prompts/<template_name>.j2."""
    return _jinja_env.get_template(f"{template_name}.j2").render(**kwargs)


def _load_agent_config() -> dict:
    with open(_CONFIG_DIR / "agent_config.yaml") as f:
        return yaml.safe_load(f)


def _init_shared_file(shared_file: Path, app_info: dict, problem_id: str) -> None:
    content = (
        "# SRE Judged Session State\n"
        "## Session\n"
        f"- Problem ID: {problem_id}\n"
        f"- App: {app_info.get('app_name', 'unknown')} "
        f"/ Namespace: {app_info.get('namespace', 'default')}\n\n"
        "## Diagnosis\n"
    )
    shared_file.write_text(content)
    logger.info(f"Initialized shared session file: {shared_file}")


def _build_prompts(
    app_info: dict,
    stage: str,
    agent_type: str,  # "agent" or "judge"
    iteration: int,
    shared_content: str,
) -> list:
    ctx = dict(
        app_name=app_info.get("app_name", "unknown"),
        namespace=app_info.get("namespace", "default"),
        descriptions=app_info.get("descriptions", ""),
        iteration=iteration,
        shared_content=shared_content,
    )
    return [
        SystemMessage(content=_render(f"{stage}_{agent_type}_system")),
        HumanMessage(content=_render(f"{stage}_{agent_type}_user", **ctx)),
    ]


async def _run_sre_agent(
    llm,
    app_info: dict,
    stage: str,
    iteration: int,
    model_name: str,
    shared_content: str,
    complete_tool,
) -> dict:
    agent = CrucibleAgent(
        llm=llm,
        tools=[read_file, write_file, str_replace_file, exec_bash, complete_tool],
        submit_tool=complete_tool,
        model_name=model_name,
    )
    return await agent.arun(_build_prompts(app_info, stage, "agent", iteration, shared_content))


async def _run_judge(
    llm,
    app_info: dict,
    stage: str,
    iteration: int,
    model_name: str,
    shared_content: str,
    approve_tool,
    reject_tool,
) -> dict:
    agent = CrucibleAgent(
        llm=llm,
        tools=[read_file, exec_bash_readonly, approve_tool, reject_tool],
        submit_tool=approve_tool,
        model_name=model_name,
    )
    return await agent.arun(_build_prompts(app_info, stage, "judge", iteration, shared_content))


async def run(
    app_info: dict,
    problem_id: str,
    shared_file: Path,
    planned_stages: list[str],
) -> None:
    """Main orchestrator: runs diagnosis (and optionally mitigation) with judge-agent loop."""
    agent_cfg = _load_agent_config()
    llm = get_llm_backend_for_tools()

    max_diag_iters = agent_cfg.get("max_diagnosis_iterations", 3)
    max_mit_iters = agent_cfg.get("max_mitigation_iterations", 3)
    wait_stage_timeout = agent_cfg.get("wait_stage_timeout", 300)
    model_name = llm.model_name

    _init_shared_file(shared_file, app_info, problem_id)

    # ── DIAGNOSIS STAGE ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("CRUCIBLE: Starting DIAGNOSIS stage")
    logger.info("=" * 60)

    diag_approved = False
    for diag_iter in range(1, max_diag_iters + 1):
        logger.info(f"--- Diagnosis iteration {diag_iter}/{max_diag_iters} ---")

        shared_content = shared_file.read_text()
        complete_tool = make_mark_hypothesis_complete(shared_file, diag_iter)
        await _run_sre_agent(llm, app_info, "diagnosis", diag_iter, model_name, shared_content, complete_tool)

        shared_content = shared_file.read_text()
        approve_tool = make_approve_and_submit(shared_file, diag_iter, "diagnosis")
        reject_tool = make_reject_with_feedback(shared_file, diag_iter, "diagnosis")
        judge_state = await _run_judge(
            llm, app_info, "diagnosis", diag_iter, model_name, shared_content, approve_tool, reject_tool,
        )

        verdict = judge_state.get("verdict")
        logger.info(f"Diagnosis iteration {diag_iter} verdict: {verdict!r}")

        if verdict == "APPROVED":
            diag_approved = True
            logger.info("Judge APPROVED diagnosis.")
            break
        else:
            logger.info(f"Judge REJECTED diagnosis (iteration {diag_iter}). Looping...")

    if not diag_approved:
        logger.warning("Max diagnosis iterations reached without APPROVED verdict.")

    if "mitigation" not in planned_stages:
        logger.info("Diagnosis-only problem — orchestrator complete.")
        return

    # Append mitigation header to shared file and wait for stage transition
    with open(shared_file, "a") as f:
        f.write("\n## Mitigation\n")

    logger.info("Waiting for benchmark to reach mitigation stage...")
    try:
        wait_for_ready_stage(timeout=wait_stage_timeout)
    except TimeoutError:
        logger.warning(f"Timed out waiting for mitigation stage after {wait_stage_timeout}s — proceeding anyway.")

    # ── MITIGATION STAGE ─────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("CRUCIBLE: Starting MITIGATION stage")
    logger.info("=" * 60)

    mit_approved = False
    for mit_iter in range(1, max_mit_iters + 1):
        logger.info(f"--- Mitigation iteration {mit_iter}/{max_mit_iters} ---")

        shared_content = shared_file.read_text()
        complete_tool = make_mark_mitigation_complete(shared_file, mit_iter)
        await _run_sre_agent(llm, app_info, "mitigation", mit_iter, model_name, shared_content, complete_tool)

        shared_content = shared_file.read_text()
        approve_tool = make_approve_and_submit(shared_file, mit_iter, "mitigation")
        reject_tool = make_reject_with_feedback(shared_file, mit_iter, "mitigation")
        judge_state = await _run_judge(
            llm, app_info, "mitigation", mit_iter, model_name, shared_content, approve_tool, reject_tool,
        )

        verdict = judge_state.get("verdict")
        logger.info(f"Mitigation iteration {mit_iter} verdict: {verdict!r}")

        if verdict == "APPROVED":
            mit_approved = True
            logger.info("Judge APPROVED mitigation.")
            break
        else:
            logger.info(f"Judge REJECTED mitigation (iteration {mit_iter}). Looping...")

    if not mit_approved:
        logger.warning("Max mitigation iterations reached without APPROVED verdict.")

    logger.info("=" * 60)
    logger.info("CRUCIBLE: Orchestrator complete.")
    logger.info("=" * 60)
