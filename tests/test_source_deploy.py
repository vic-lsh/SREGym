from sregym.service import source_deploy


def test_docker_build_uses_configured_builder(monkeypatch) -> None:
    commands = []
    monkeypatch.setenv("SREGYM_DOCKER_BUILDER", "healthy-builder")
    monkeypatch.setattr(source_deploy.subprocess, "run", lambda command, check: commands.append(command))

    source_deploy._run_command(["docker", "build", "-t", "image", "."])

    assert commands == [
        ["docker", "buildx", "build", "--builder", "healthy-builder", "--load", "-t", "image", "."]
    ]
