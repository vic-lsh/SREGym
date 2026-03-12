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
    lt_summary_file: Path | None = None,
) -> list:
    ctx = dict(
        app_name=app_info.get("app_name", "unknown"),
        namespace=app_info.get("namespace", "default"),
        descriptions=app_info.get("descriptions", ""),
        iteration=iteration,
        shared_content=shared_content,
        shared_file=str(shared_file),
        lt_summary_file=str(lt_summary_file) if lt_summary_file else None,
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
    lt_summary_file: Path | None = None,
) -> dict:
    agent = CrucibleAgent(
        llm=llm,
        tools=[read_file, write_file, str_replace_file, exec_bash, complete_tool],
        submit_tool=complete_tool,
        model_name=model_name,
        role=f"{stage}-agent",
    )
    return await agent.arun(_build_prompts(app_info, stage, "agent", iteration, shared_content, shared_file, lt_summary_file=lt_summary_file))


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


def _build_usage_result(usage_by_agent: dict) -> dict:
    total = {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0}
    for agent_data in usage_by_agent.values():
        total = _add_usage(total, agent_data["total"])
    return {"by_agent": usage_by_agent, "total": total}


def _zero_usage() -> dict:
    return {"input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0}


def _add_usage(a: dict, b: dict) -> dict:
    return {k: a[k] + b.get(k, 0) for k in a}


async def _run_stage_loop(
    llm,
    app_info: dict,
    stage: str,
    max_iters: int,
    model_name: str,
    shared_file: Path,
    make_complete_tool,
    lt_summary_file: Path | None = None,
) -> tuple[bool, dict]:
    """Run the agent→judge loop for one stage. Returns (approved, usage_by_role)."""
    logger.info("=" * 60)
    logger.info(f"CRUCIBLE: Starting {stage.upper()} stage")
    logger.info("=" * 60)

    agent_role = f"{stage}-agent"
    judge_role = f"{stage}-judge"
    usage_by_role: dict[str, dict] = {
        agent_role: {"iterations": [], "total": _zero_usage()},
        judge_role: {"iterations": [], "total": _zero_usage()},
    }

    for iteration in range(1, max_iters + 1):
        logger.info(f"--- {stage.capitalize()} iteration {iteration}/{max_iters} ---")

        shared_content = shared_file.read_text()
        complete_tool = make_complete_tool(shared_file, iteration)
        agent_state = await _run_sre_agent(llm, app_info, stage, iteration, model_name, shared_content, shared_file, complete_tool, lt_summary_file=lt_summary_file)
        agent_usage = agent_state.get("usage", _zero_usage())
        usage_by_role[agent_role]["iterations"].append(agent_usage)
        usage_by_role[agent_role]["total"] = _add_usage(usage_by_role[agent_role]["total"], agent_usage)

        shared_content = shared_file.read_text()
        verdict_tool = make_submit_verdict(shared_file, iteration, stage)
        judge_state = await _run_judge(
            llm, app_info, stage, iteration, model_name, shared_content, shared_file, verdict_tool,
        )
        judge_usage = judge_state.get("usage", _zero_usage())
        usage_by_role[judge_role]["iterations"].append(judge_usage)
        usage_by_role[judge_role]["total"] = _add_usage(usage_by_role[judge_role]["total"], judge_usage)

        verdict = judge_state.get("verdict")
        logger.info(f"{stage.capitalize()} iteration {iteration} verdict: {verdict!r}")

        if verdict == "APPROVED":
            logger.info(f"Judge APPROVED {stage}.")
            return True, usage_by_role
        logger.info(f"Judge REJECTED {stage} (iteration {iteration}). Looping...")

    logger.warning(f"Max {stage} iterations reached without APPROVED verdict.")
    return False, usage_by_role


async def run(
    app_info: dict,
    problem_id: str,
    shared_file: Path,
    planned_stages: list[str],
    lt_summary_file: Path | None = None,
) -> dict:
    """Main orchestrator: runs diagnosis (and optionally mitigation) with judge-agent loop."""
    agent_cfg = _load_agent_config()
    llm = get_llm_backend_for_tools()

    max_diag_iters = agent_cfg.get("max_diagnosis_iterations", 3)
    max_mit_iters = agent_cfg.get("max_mitigation_iterations", 3)
    wait_stage_timeout = agent_cfg.get("wait_stage_timeout", 300)
    model_name = llm.model_name

    _init_shared_file(shared_file, app_info, problem_id)

    # Resolve to absolute paths once so agents receive stable absolute paths in prompts.
    shared_file = shared_file.resolve()
    lt_summary_file = lt_summary_file.resolve() if lt_summary_file else None

    _, diag_usage = await _run_stage_loop(llm, app_info, "diagnosis", max_diag_iters, model_name, shared_file, make_mark_hypothesis_complete, lt_summary_file=lt_summary_file)
    usage_by_agent = diag_usage

    if "mitigation" not in planned_stages:
        logger.info("Diagnosis-only problem — orchestrator complete.")
        return _build_usage_result(usage_by_agent)

    with open(shared_file, "a") as f:
        f.write("\n## Mitigation\n")

    logger.info("Waiting for benchmark to reach mitigation stage...")
    try:
        wait_for_ready_stage(timeout=wait_stage_timeout)
    except TimeoutError:
        logger.warning(f"Timed out waiting for mitigation stage after {wait_stage_timeout}s — proceeding anyway.")

    _, mit_usage = await _run_stage_loop(llm, app_info, "mitigation", max_mit_iters, model_name, shared_file, make_mark_mitigation_complete, lt_summary_file=lt_summary_file)
    usage_by_agent = {**diag_usage, **mit_usage}

    logger.info("=" * 60)
    logger.info("CRUCIBLE: Orchestrator complete.")
    logger.info("=" * 60)
    return _build_usage_result(usage_by_agent)
