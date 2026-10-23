"""The high-CPU injector must send a valid Kubernetes strategic merge patch."""

import json
import shlex
from types import SimpleNamespace

from sregym.conductor.problems.ad_service_high_cpu import AdServiceHighCpu


def test_high_cpu_injection_sends_a_valid_cpu_limit_patch() -> None:
    commands: list[str] = []
    flags: list[str] = []
    # Exercise the injector without its application constructor or cluster access.
    problem = object.__new__(AdServiceHighCpu)
    problem.namespace = "synthetic-shop"
    problem.faulty_service = "ad"
    problem.feature_flag = "adHighCpu"
    problem.cpu_limit = "100m"
    problem.injector = SimpleNamespace(inject_fault=flags.append)
    problem.kubectl = SimpleNamespace(exec_command=commands.append)

    problem.inject_fault()

    assert flags == ["adHighCpu"]
    argv = shlex.split(commands[0])
    patch = json.loads(argv[argv.index("-p") + 1])
    assert patch["spec"]["template"]["spec"]["containers"] == [{"name": "ad", "resources": {"limits": {"cpu": "100m"}}}]
