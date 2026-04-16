from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml


DEFAULT_REG_PATH = Path(os.environ.get("SREGYM_AGENT_REGISTRY", "agents.yaml"))


@dataclass
class AgentRegistration:
    name: str
    kickoff_command: str | None = None
    kickoff_workdir: str | None = None
    kickoff_env: dict[str, str] | None = None
    # Optional opt-in: wait for the agent to exit naturally after the
    # conductor reaches "done" instead of applying the generic timeout.
    wait_for_natural_exit: bool | None = None
    # Optional opt-in: after the final stage is evaluated, hold teardown
    # (recover_fault + undeploy + reconcile) until the agent POSTs /cleanup.
    # Agents that do post-submit reflection against the live cluster (e.g.
    # crucible's recovery-diagnosis + playbook generation) need this. Without
    # it, the conductor tears down synchronously on the final /submit and the
    # reflection step sees a deleted namespace. sregym reads this flag at
    # startup, passes it to Conductor(defer_cleanup=...), and injects
    # SREGYM_DEFER_CLEANUP=1 into the agent subprocess env so the agent knows
    # to POST /cleanup when its post-submit work is complete.
    defer_cleanup: bool | None = None


def _ensure_file(path: Path) -> None:
    if not path.exists():
        path.write_text(yaml.safe_dump({"agents": []}, sort_keys=False))


def list_agents(path: Path = DEFAULT_REG_PATH) -> dict[str, AgentRegistration]:
    _ensure_file(path)
    data = yaml.safe_load(path.read_text()) or {"agents": []}
    out: dict[str, AgentRegistration] = {}
    for agent_data in data.get("agents", []):
        out[agent_data["name"]] = AgentRegistration(
            name=agent_data["name"],
            kickoff_command=agent_data.get("kickoff_command"),
            kickoff_workdir=agent_data.get("kickoff_workdir"),
            kickoff_env=agent_data.get("kickoff_env") or {},
            wait_for_natural_exit=agent_data.get("wait_for_natural_exit"),
            defer_cleanup=agent_data.get("defer_cleanup"),
        )
    return out


def get_agent(name: str, path: Path = DEFAULT_REG_PATH) -> AgentRegistration | None:
    return list_agents(path).get(name)


def save_agent(reg: AgentRegistration, path: Path = DEFAULT_REG_PATH) -> None:
    _ensure_file(path)
    data = yaml.safe_load(path.read_text()) or {"agents": []}
    agents = [agent_data for agent_data in data.get("agents", []) if agent_data.get("name") != reg.name]
    agents.append(asdict(reg))
    data["agents"] = agents
    path.write_text(yaml.safe_dump(data, sort_keys=False))
