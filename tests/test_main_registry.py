from pathlib import Path

import main


def test_agent_registry_path_honors_launcher_override(monkeypatch) -> None:
    monkeypatch.setenv("SREGYM_AGENT_REGISTRY", "/tmp/sdo-registry.yaml")
    assert main._agent_registry_path() == Path("/tmp/sdo-registry.yaml")
