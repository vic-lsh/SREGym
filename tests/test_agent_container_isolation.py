"""The agent container sees the cluster only through the filtering proxy, and
its image carries only the agent drivers: no fault definitions or graders."""

from __future__ import annotations

import ast
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

from sregym.service.container_runner import ContainerConfig, ContainerRunner, ExecInput

REPO_ROOT = Path(__file__).resolve().parents[1]

KUBECONFIG_TEXT = "apiVersion: v1\nkind: Config\nclusters:\n- name: {name}\n"

# Benchmark internals the agent must never read.
FORBIDDEN_NAMES = {"sregym", "conductor", "problems", "oracles", "generators", "SREGym-applications"}
HOST_ONLY_CLIENTS = {"demo", "tierzero"}


def _kubeconfig(path: Path, name: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(KUBECONFIG_TEXT.format(name=name))
    return path


def _docker_command(tmp_path: Path, monkeypatch) -> tuple[list[str], Path, Path]:
    """docker run argv for a host with a real kubeconfig everywhere one is looked for."""
    home = tmp_path / "home"
    real = _kubeconfig(home / ".kube" / "config", "real")
    elsewhere = _kubeconfig(tmp_path / "other-kubeconfig", "real-too")
    (home / ".aws").mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("KUBECONFIG", str(elsewhere))
    monkeypatch.setenv("SREGYM_REAL_KUBECONFIG", str(real))
    proxy = _kubeconfig(tmp_path / "proxy" / "agent-kubeconfig", "proxy")

    runner = ContainerRunner(ContainerConfig(kubeconfig_path=proxy, logs_path=tmp_path / "logs"))
    try:
        cmd = runner.build_docker_command(ExecInput(command="true", label="t"))
    finally:
        runner.cleanup_credential_tmps()
    return cmd, proxy, real


def _flag_values(cmd: list[str], flag: str) -> list[str]:
    return [cmd[i + 1] for i, arg in enumerate(cmd[:-1]) if arg == flag]


def test_the_only_kubeconfig_reachable_from_the_container_is_the_proxys(tmp_path, monkeypatch):
    cmd, proxy, _ = _docker_command(tmp_path, monkeypatch)

    mounted_kubeconfigs = set()
    for volume in _flag_values(cmd, "-v"):
        source = Path(volume.split(":", 1)[0])
        files = [source] if source.is_file() else [p for p in source.rglob("*") if p.is_file()]
        mounted_kubeconfigs |= {f for f in files if "kind: Config" in f.read_text(errors="ignore")}

    assert mounted_kubeconfigs == {proxy.resolve()}


def test_no_real_kubeconfig_reference_is_passed_into_the_container(tmp_path, monkeypatch):
    cmd, _, real = _docker_command(tmp_path, monkeypatch)

    env = dict(e.split("=", 1) for e in _flag_values(cmd, "-e"))
    assert "SREGYM_REAL_KUBECONFIG" not in env
    assert env.get("KUBECONFIG") == "/root/.kube/config"
    assert not any(str(real) in arg or "real-config" in arg for arg in cmd)


def test_grader_configuration_stays_on_the_host(tmp_path, monkeypatch):
    for var in ("JUDGE_MODEL_ID", "JUDGE_API_BASE", "JUDGE_API_KEY"):
        monkeypatch.setenv(var, "judge-only")
    cmd, _, _ = _docker_command(tmp_path, monkeypatch)

    env = dict(e.split("=", 1) for e in _flag_values(cmd, "-e"))
    assert [k for k in env if k.startswith("JUDGE_")] == []


def _build_context(tmp_path: Path) -> Path:
    ctx = tmp_path / "ctx"
    subprocess.run(["bash", str(REPO_ROOT / "docker/agents/build.sh"), "--context", str(ctx)], check=True)
    return ctx


def test_image_build_context_has_no_answer_key_material(tmp_path):
    ctx = _build_context(tmp_path)

    forbidden = [p for p in ctx.rglob("*") if p.stem in FORBIDDEN_NAMES]
    host_only = [p for p in (ctx / "clients").iterdir() if p.name in HOST_ONLY_CLIENTS]
    assert forbidden == []
    assert host_only == []


def _containerized_kickoff_modules() -> dict[str, str]:
    agents = yaml.safe_load((REPO_ROOT / "agents.yaml").read_text())["agents"]
    modules = {}
    for agent in agents:
        argv = shlex.split(agent.get("kickoff_command") or "")
        if agent.get("container_isolation", True) and argv[:2] == ["python", "-m"]:
            modules[agent["name"]] = argv[2]
    return modules


def _module_file(root: Path, module: str) -> Path | None:
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _imports(path: Path, module: str) -> set[str]:
    """Every module path named by an import in the file, lazy ones included."""
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    names = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                anchor = package.split(".")[: len(package.split(".")) - node.level + 1]
                base = ".".join([*anchor, base] if base else anchor)
            names.add(base)
            names |= {f"{base}.{alias.name}" for alias in node.names}
    return names


def _missing_first_party_imports(ctx: Path, entry: str) -> set[str]:
    """First-party modules reachable from entry by imports but absent from ctx."""
    first_party = {p.stem for p in REPO_ROOT.iterdir() if _module_file(REPO_ROOT, p.stem)}
    seen, missing, pending = set(), set(), [entry]
    while pending:
        module = pending.pop()
        if module in seen or module.split(".")[0] not in first_party:
            continue
        seen.add(module)
        parts = module.split(".")
        pending += [".".join(parts[:i]) for i in range(1, len(parts))]
        path = _module_file(ctx, module)
        if path is None:
            # "from pkg import name" names an attribute unless pkg.name is a module.
            if _module_file(REPO_ROOT, module) is not None:
                missing.add(module)
            continue
        pending += _imports(path, module)
    return missing


@pytest.mark.parametrize("agent, module", sorted(_containerized_kickoff_modules().items()))
def test_every_containerized_driver_imports_only_what_the_image_ships(tmp_path, agent, module):
    ctx = _build_context(tmp_path)

    assert _module_file(ctx, module) is not None, f"{agent}: {module} is not in the image"
    assert _missing_first_party_imports(ctx, module) == set()
