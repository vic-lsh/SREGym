"""SREGym runs agents only from an agent image built from this checkout's agent code that passed check_image.sh."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from sregym.service import container_runner
from sregym.service.container_runner import (
    AGENT_IMAGE_REPO,
    CHECK_SCRIPT,
    CONTEXT_LABEL,
    ContainerConfig,
    ContainerRunner,
    ExecInput,
    tree_hash,
)

HASH = "0123456789abcdef"


class _FakeDocker:
    """Stands in for the docker CLI, build.sh and check_image.sh."""

    def __init__(self, images=None, check_ok=True):
        self.images = dict(images or {})  # tag -> (image id, context label)
        self.check_ok = check_ok
        self.calls = []

    def run(self, argv, **kwargs):
        argv = [str(a) for a in argv]
        if argv[:3] == ["docker", "image", "inspect"]:
            self.calls.append(("inspect", argv[3]))
            if argv[3] not in self.images:
                return subprocess.CompletedProcess(argv, 1, "", "No such image")
            image_id, label = self.images[argv[3]]
            labels = {CONTEXT_LABEL: label} if label is not None else None
            return subprocess.CompletedProcess(argv, 0, json.dumps([{"Id": image_id, "Config": {"Labels": labels}}]))
        if argv[:2] == ["bash", str(CHECK_SCRIPT)]:
            self.calls.append(("check", argv[2]))
            return subprocess.CompletedProcess(argv, 0 if self.check_ok else 1, "", "FAIL: forbidden path")
        if argv[:2] == ["bash", str(container_runner.BUILD_SCRIPT)]:
            env = kwargs["env"]
            self.calls.append(("build", env["SREGYM_AGENT_IMAGE"], env["SREGYM_AGENT_CONTEXT_HASH"]))
            self.images[env["SREGYM_AGENT_IMAGE"]] = ("sha256:built", env["SREGYM_AGENT_CONTEXT_HASH"])
            return subprocess.CompletedProcess(argv, 0)
        raise AssertionError(f"unexpected command {argv}")


@pytest.fixture
def docker(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(container_runner, "agent_context_hash", lambda: HASH)
    monkeypatch.setattr(container_runner, "_VERIFIED_IMAGE_IDS", set())
    fake = _FakeDocker()
    monkeypatch.setattr(container_runner.subprocess, "run", fake.run)
    return fake


def _docker_run_image(runner):
    return runner.build_docker_command(ExecInput(command="true"))[-2]


def test_the_default_image_is_tagged_with_the_build_context_hash(docker):
    assert ContainerRunner().image == f"{AGENT_IMAGE_REPO}:{HASH}"
    assert AGENT_IMAGE_REPO != "sregym-agent-base"


def test_a_current_image_is_checked_once_per_process_and_run_by_id(docker):
    docker.images[f"{AGENT_IMAGE_REPO}:{HASH}"] = ("sha256:current", HASH)

    first, second = ContainerRunner(), ContainerRunner()
    first.ensure_image_exists()
    second.ensure_image_exists()

    assert [c for c in docker.calls if c[0] != "inspect"] == [("check", "sha256:current")]
    assert _docker_run_image(first) == _docker_run_image(second) == "sha256:current"


@pytest.mark.parametrize("label", [None, "fedcba9876543210"])
def test_an_image_with_a_missing_or_other_context_label_is_rebuilt(docker, label):
    tag = "sregym-agent-base:latest"
    docker.images[tag] = ("sha256:stale", label)

    runner = ContainerRunner(ContainerConfig(image=tag))
    runner.ensure_image_exists()

    assert ("build", tag, HASH) in docker.calls
    assert docker.calls[-1] == ("check", "sha256:built")
    assert _docker_run_image(runner) == "sha256:built"


def test_a_missing_image_is_built_and_force_build_rebuilds_a_current_one(docker):
    tag = f"{AGENT_IMAGE_REPO}:{HASH}"
    ContainerRunner().ensure_image_exists()
    assert ("build", tag, HASH) in docker.calls

    docker.calls.clear()
    ContainerRunner().ensure_image_exists(force_build=True)
    assert ("build", tag, HASH) in docker.calls


def test_an_image_failing_the_isolation_check_never_runs(docker):
    docker.images[f"{AGENT_IMAGE_REPO}:{HASH}"] = ("sha256:fat", HASH)
    docker.check_ok = False
    runner = ContainerRunner()

    with pytest.raises(RuntimeError, match="failed the isolation check"):
        runner.ensure_image_exists()
    with pytest.raises(RuntimeError, match="not checked"):
        runner.build_docker_command(ExecInput(command="true"))


def test_the_context_hash_changes_with_any_file_path_mode_or_content(tmp_path):
    root = tmp_path / "ctx"
    (root / "clients").mkdir(parents=True)
    driver = root / "clients" / "driver.py"
    driver.write_text("print('a')\n")
    hashes = {tree_hash(root)}

    driver.write_text("print('b')\n")
    hashes.add(tree_hash(root))
    driver.chmod(0o755)
    hashes.add(tree_hash(root))
    driver.rename(root / "clients" / "other.py")
    hashes.add(tree_hash(root))

    assert len(hashes) == 4
    assert tree_hash(root) == tree_hash(Path(root))


def test_the_real_context_hash_is_stable():
    assert container_runner.agent_context_hash() == container_runner.agent_context_hash()
