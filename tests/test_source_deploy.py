import subprocess
from pathlib import Path

import pytest

from sregym.service import source_deploy


def test_docker_build_uses_configured_builder(monkeypatch) -> None:
    commands = []
    monkeypatch.setenv("SREGYM_DOCKER_BUILDER", "healthy-builder")
    monkeypatch.setattr(source_deploy.subprocess, "run", lambda command, check: commands.append(command))

    source_deploy._run_command(["docker", "build", "-t", "image", "."])

    assert commands == [["docker", "buildx", "build", "--builder", "healthy-builder", "--load", "-t", "image", "."]]


def _hotel_source(root: Path) -> Path:
    (root / "cmd").mkdir(parents=True)
    (root / "cmd" / "main.go").write_text("package main\n")
    (root / "Dockerfile").write_text("FROM golang\nCOPY . /src\n")
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (root / ".sdo").mkdir()
    (root / ".sdo" / "goal.md").write_text("keep it up\n")
    return root


def test_context_digest_ignores_repository_metadata_and_tracks_sources(tmp_path: Path) -> None:
    source = _hotel_source(tmp_path / "app")
    digest = source_deploy.source_context_digest(source)

    (source / ".git" / "HEAD").write_text("ref: refs/heads/other\n")
    (source / ".sdo" / "playbook.md").write_text("new playbook\n")
    assert source_deploy.source_context_digest(source) == digest

    (source / "cmd" / "main.go").write_text("package main // changed\n")
    assert source_deploy.source_context_digest(source) != digest


class FakeDocker:
    def __init__(self, labels: dict[str, str]) -> None:
        self.labels = labels
        self.commands: list[list[str]] = []

    def run(self, command, check=False, capture_output=False, text=False):
        self.commands.append(list(command))
        if command[:3] == ["docker", "image", "inspect"]:
            ref = command[-1]
            if ref not in self.labels:
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="No such image")
            return subprocess.CompletedProcess(command, 0, stdout=self.labels[ref] + "\n", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    def kinds(self) -> list[str]:
        return [" ".join(command[:3]) for command in self.commands]


@pytest.fixture
def hotel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, FakeDocker, object]:
    source = _hotel_source(tmp_path / "hotel")
    monkeypatch.delenv("SREGYM_DOCKER_BUILDER", raising=False)
    monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
    adapter = source_deploy._HotelReservationAdapter()
    monkeypatch.setattr(adapter, "_source_dir", lambda: source)
    docker = FakeDocker({})
    monkeypatch.setattr(source_deploy.subprocess, "run", docker.run)
    return source, docker, adapter


def _image_ref() -> str:
    return f"yinfangchen/hotelreservation:{source_deploy.source_image_tag('Hotel Reservation', 'fastloop-w0')}"


def test_unchanged_build_context_skips_the_rebuild_but_still_loads_the_cluster(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    docker.labels[_image_ref()] = source_deploy.source_context_digest(source)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert docker.kinds() == ["docker image inspect", "kind load docker-image"]


def test_changed_build_context_rebuilds_with_the_digest_label(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    docker.labels[_image_ref()] = "stale-digest"

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    build = next(command for command in docker.commands if command[:2] == ["docker", "build"])
    assert f"sregym.source-digest={source_deploy.source_context_digest(source)}" in build
    assert build[build.index("--label") + 1].startswith("sregym.source-digest=")
    assert docker.kinds()[-1] == "kind load docker-image"


def test_build_cache_is_opt_in(hotel, monkeypatch) -> None:
    _source, docker, adapter = hotel
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert docker.kinds() == ["docker build -t", "kind load docker-image"]
    assert "--label" not in docker.commands[0]
