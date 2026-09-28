from pathlib import Path

import yaml

from sregym.agent_registry import list_agents
from sregym.conductor.conductor import Conductor, ConductorConfig


def test_agent_registry_preserves_cleanup_contract(tmp_path: Path) -> None:
    registry = tmp_path / "agents.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "agents": [
                    {
                        "name": "sdo_codex",
                        "container_isolation": False,
                        "defer_cleanup": True,
                        "wait_for_natural_exit": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registration = list_agents(registry)["sdo_codex"]
    assert registration.defer_cleanup is True
    assert registration.wait_for_natural_exit is True


def test_final_stage_defers_teardown_until_cleanup_signal(monkeypatch) -> None:
    conductor = Conductor.__new__(Conductor)
    conductor.config = ConductorConfig(defer_cleanup=True)
    conductor.stage_sequence = [{"name": "diagnosis"}]
    conductor.waiting_for_agent = True
    conductor.current_stage_index = 0
    conductor.fault_injected = True
    conductor.submission_stage = "diagnosis"
    conductor.logger = type("Logger", (), {"info": lambda *args: None})()
    started = []
    monkeypatch.setattr(conductor, "_start_cleanup_watchdog", lambda: started.append(True))

    conductor._advance_to_next_stage(start_index=1)

    assert conductor.submission_stage == "awaiting_cleanup"
    assert started == [True]


def test_a_recovery_error_is_recorded_and_teardown_still_runs() -> None:
    """A composite raises its components' recovery errors; the conductor must not stop at them."""
    events: list[str] = []

    class _App:
        def cleanup(self) -> None:
            events.append("undeploy")

    class _Problem:
        app = _App()

        def recover_fault(self) -> None:
            events.append("recover")
            raise ExceptionGroup("composite_x: recovery failed for a", [RuntimeError("a failed")])

    logged: list[str] = []
    conductor = Conductor.__new__(Conductor)
    conductor.config = ConductorConfig()
    conductor.problem = _Problem()
    conductor.results = {}
    conductor._baseline_captured = False
    conductor.logger = type(
        "Logger", (), {"info": lambda *args: None, "exception": lambda _self, message: logged.append(message)}
    )()

    conductor._cleanup_sync()

    assert events == ["recover", "undeploy"]
    assert conductor.submission_stage == "done"
    assert "composite_x: recovery failed for a" in conductor.results["fault_recovery_error"]
    assert logged
