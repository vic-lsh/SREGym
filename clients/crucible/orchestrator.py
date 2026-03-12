import logging
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from langchain_core.messages import HumanMessage, SystemMessage

from clients.common.driver_utils import wait_for_ready_stage
from clients.crucible.agents.agent import CrucibleAgent
from clients.crucible.tools.bash_tool import exec_bash, exec_bash_readonly
from clients.crucible.tools.file_tools import read_file, str_replace_file, write_file
from clients.crucible.tools.judge_tools import make_submit_verdict
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
    shared_file: Path,
) -> list:
    ctx = dict(
        app_name=app_info.get("app_name", "unknown"),
        namespace=app_info.get("namespace", "default"),
        descriptions=app_info.get("descriptions", ""),
        iteration=iteration,
        shared_content=shared_content,
        shared_file=str(shared_file.resolve()),
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
    shared_file: Path,
    complete_tool,
) -> dict:
    agent = CrucibleAgent(
        llm=llm,
        tools=[read_file, write_file, str_replace_file, exec_bash, complete_tool],
        submit_tool=complete_tool,
        model_name=model_name,
        role=f"{stage}-agent",
    )
    return await agent.arun(_build_prompts(app_info, stage, "agent", iteration, shared_content, shared_file))


async def _run_judge(
    llm,
    app_info: dict,
    stage: str,
    iteration: int,
    model_name: str,
    shared_content: str,
    shared_file: Path,
    verdict_tool,
) -> dict:
    agent = CrucibleAgent(
        llm=llm,
        tools=[read_file, exec_bash_readonly, verdict_tool],
        submit_tool=verdict_tool,
        model_name=model_name,
        role=f"{stage}-judge",
    )
    return await agent.arun(_build_prompts(app_info, stage, "judge", iteration, shared_content, shared_file))


async def _run_stage_loop(
    llm,
    app_info: dict,
    stage: str,
    max_iters: int,
    model_name: str,
    shared_file: Path,
    make_complete_tool,
) -> bool:
    """Run the agent→judge loop for one stage. Returns True if judge approved."""
    logger.info("=" * 60)
    logger.info(f"CRUCIBLE: Starting {stage.upper()} stage")
    logger.info("=" * 60)

    for iteration in range(1, max_iters + 1):
        logger.info(f"--- {stage.capitalize()} iteration {iteration}/{max_iters} ---")

        shared_content = shared_file.read_text()
        complete_tool = make_complete_tool(shared_file, iteration)
        await _run_sre_agent(llm, app_info, stage, iteration, model_name, shared_content, shared_file, complete_tool)

        shared_content = shared_file.read_text()
        verdict_tool = make_submit_verdict(shared_file, iteration, stage)
        judge_state = await _run_judge(
            llm, app_info, stage, iteration, model_name, shared_content, shared_file, verdict_tool,
        )

        verdict = judge_state.get("verdict")
        logger.info(f"{stage.capitalize()} iteration {iteration} verdict: {verdict!r}")

        if verdict == "APPROVED":
            logger.info(f"Judge APPROVED {stage}.")
            return True
        logger.info(f"Judge REJECTED {stage} (iteration {iteration}). Looping...")

    logger.warning(f"Max {stage} iterations reached without APPROVED verdict.")
    return False


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

    await _run_stage_loop(llm, app_info, "diagnosis", max_diag_iters, model_name, shared_file, make_mark_hypothesis_complete)

    if "mitigation" not in planned_stages:
        logger.info("Diagnosis-only problem — orchestrator complete.")
        return

    with open(shared_file, "a") as f:
        f.write("\n## Mitigation\n")

    logger.info("Waiting for benchmark to reach mitigation stage...")
    try:
        wait_for_ready_stage(timeout=wait_stage_timeout)
    except TimeoutError:
        logger.warning(f"Timed out waiting for mitigation stage after {wait_stage_timeout}s — proceeding anyway.")

    await _run_stage_loop(llm, app_info, "mitigation", max_mit_iters, model_name, shared_file, make_mark_mitigation_complete)

    logger.info("=" * 60)
    logger.info("CRUCIBLE: Orchestrator complete.")
    logger.info("=" * 60)
