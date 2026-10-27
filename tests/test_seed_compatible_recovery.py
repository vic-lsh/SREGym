"""Synthetic credential fault contracts; all credential values are fake placeholders."""

from types import SimpleNamespace

import pytest

from sregym.fastloop.worker import FastloopWorker

EMPTY_INVENTORY = SimpleNamespace(list_objects=lambda namespaces: frozenset(), delete=lambda obj: None)


class CredentialProblem:
    namespace = "toy"
    app = SimpleNamespace(namespace="toy")

    def __init__(self):
        self.events = []
        self.backend = "fake-seed"

    def inject_fault(self):
        self.backend = "fake-rotated"

    def recover_fault(self):
        self.events.append("ordinary-recovery")
        self.backend = "fake-rotated"

    def recover_to_baseline(self):
        self.events.append("baseline-recovery")
        self.backend = "fake-seed"

    def refresh_after_recovery(self):
        self.events.append("refresh")


def worker(problem):
    return FastloopWorker(
        problem_factory=lambda _: problem,
        deployer=lambda *_: None,
        health_probe=lambda _: {"healthy": True},
        cluster_factory=lambda *_: ("toy", "/unused"),
        cluster_inventory=EMPTY_INVENTORY,
    )


def test_full_recovery_uses_baseline_contract_and_is_retryable():
    problem = CredentialProblem()
    client = worker(problem)
    client.handle({"op": "inject", "problem_id": "toy"})
    for _ in range(2):
        result = client.handle({"op": "recover", "mode": "full"})
        assert result["success"]
        assert result["recovery_contract"] == "seed-compatible/v1"
        assert problem.backend == "fake-seed"
        refreshed = client.handle({"op": "refresh"})
        assert refreshed["success"]
    assert problem.events == ["baseline-recovery", "refresh", "refresh"]


def test_refresh_cannot_run_before_successful_recovery():
    client = worker(CredentialProblem())
    client.handle({"op": "inject", "problem_id": "toy"})
    with pytest.raises(RuntimeError, match="recovery"):
        client.handle({"op": "refresh"})


def test_credential_problem_public_reset_reuses_harness_baseline_then_refreshes_all_clients():
    from sregym.conductor.problems.secret_rotation_stale_env_credentials import (
        SecretRotationStaleEnvCredentialsAstronomyShop,
    )

    problem = SecretRotationStaleEnvCredentialsAstronomyShop.__new__(SecretRotationStaleEnvCredentialsAstronomyShop)
    events = []
    problem.old_password = "fake-seed"
    problem.literal_db_clients = {"toy-accounting": None, "toy-reviews": None}
    problem.faulty_service = "toy-catalog"
    problem._recover_to_baseline = lambda: events.append("baseline")
    problem._drop_postgres_connections = lambda _: events.append("disconnect")
    problem._rollout_restart = lambda name: events.append(name)
    for _ in range(2):
        problem.recover_to_baseline()
        events.append("seed")
        problem.refresh_after_recovery()
    assert events == ["baseline", "seed", "disconnect", "toy-accounting", "toy-reviews", "toy-catalog"] * 2


@pytest.mark.parametrize("failure", [False, True])
def test_credential_command_never_emits_sensitive_io(monkeypatch, caplog, capsys, failure):
    from sregym.conductor.problems import secret_rotation_stale_env_credentials as module

    problem = module.SecretRotationStaleEnvCredentialsAstronomyShop.__new__(
        module.SecretRotationStaleEnvCredentialsAstronomyShop
    )
    canary = "FAKE-CREDENTIAL-CANARY"
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1 if failure else 0, stdout=canary, stderr=canary),
    )
    caplog.set_level("DEBUG")
    if failure:
        with pytest.raises(RuntimeError) as error:
            problem._run(f"kubectl fake {canary}", input_data=canary)
        assert canary not in str(error.value)
    else:
        assert problem._run(f"kubectl fake {canary}", input_data=canary) == canary
    output = capsys.readouterr()
    assert canary not in caplog.text + output.out + output.err


def test_real_credential_reset_is_idempotent_and_restores_seed_before_clients():
    from sregym.conductor.problems.secret_rotation_stale_env_credentials import (
        SecretRotationStaleEnvCredentialsAstronomyShop,
    )

    problem = SecretRotationStaleEnvCredentialsAstronomyShop.__new__(SecretRotationStaleEnvCredentialsAstronomyShop)
    problem.old_password = "fake-seed"
    problem.new_password = "fake-rotated"
    problem.old_conn = "fake-seed-connection"
    problem.namespace = "toy"
    problem.secret_name = "toy-db-conn"
    problem.faulty_service = "toy-catalog"
    state = {"backend": "fake-rotated"}
    events = []

    def rotate(previous, target):
        assert state["backend"] == previous
        state["backend"] = target
        events.append("reset-backend")

    def clients(_):
        assert state["backend"] == "fake-seed"
        events.append("restore-clients")

    problem._postgres_accepts_password = lambda candidate: candidate == state["backend"]
    problem._rotate_postgres_password = rotate
    problem._patch_postgresql_init_password = lambda _: events.append("restore-init")
    problem._set_literal_db_clients_password = clients
    problem._set_product_catalog_literal_env = lambda _: events.append("restore-catalog")
    problem._run = lambda _: events.append("remove-secret")
    problem._mark_secret_state = lambda state: events.append(f"secret-state-{state}")
    problem._rollout_restart = lambda _: events.append("rollout")
    problem._clear_stale_product_catalog_pod_uid = lambda: events.append("clear-marker")
    problem.recover_to_baseline()
    problem.recover_to_baseline()
    assert (
        events
        == ["reset-backend"]
        + [
            "restore-init",
            "restore-clients",
            "restore-catalog",
            "remove-secret",
            "secret-state-None",
            "rollout",
            "clear-marker",
        ]
        * 2
    )


@pytest.mark.parametrize("phase", ["recover_to_baseline", "refresh_after_recovery"])
def test_credential_hook_failure_does_not_return_sensitive_exceptions(phase):
    from sregym.conductor.problems.secret_rotation_stale_env_credentials import (
        SecretRotationStaleEnvCredentialsAstronomyShop,
    )

    problem = SecretRotationStaleEnvCredentialsAstronomyShop.__new__(SecretRotationStaleEnvCredentialsAstronomyShop)
    problem.old_password = "fake-seed"

    def fail(*args):
        raise RuntimeError("FAKE-CREDENTIAL-CANARY")

    problem._recover_to_baseline = fail
    problem._drop_postgres_connections = fail
    client = worker(problem)
    # Install a synthetic in-flight fault without calling the real injector.
    client._incidents = [problem]
    client._faults = [(0, None)]
    client._components = [{"component_index": 0, "injection_order": 0, "identity": {}}]
    if phase == "refresh_after_recovery":
        client._recovered = {0}
    result = client.handle({"op": "recover" if phase == "recover_to_baseline" else "refresh"})
    assert not result["success"]
    assert "FAKE-CREDENTIAL-CANARY" not in str(result)


def test_refresh_failure_is_retryable_and_blocks_new_injection():
    problem = CredentialProblem()
    client = worker(problem)
    client.handle({"op": "inject", "problem_id": "toy"})
    client.handle({"op": "recover"})

    def fail():
        raise RuntimeError("synthetic rollout unavailable")

    problem.refresh_after_recovery = fail
    assert not client.handle({"op": "refresh"})["success"]
    with pytest.raises(RuntimeError, match="recovery"):
        client.handle({"op": "inject", "problem_id": "next"})
    problem.refresh_after_recovery = lambda: None
    assert client.handle({"op": "refresh"})["success"]
    client.handle({"op": "inject", "problem_id": "next"})


def test_refresh_timeout_permanently_quarantines_the_worker():
    import threading

    problem = CredentialProblem()
    problem.recover_timeout_s = 0.01
    done = threading.Event()
    problem.refresh_after_recovery = lambda: done.wait(1)
    client = worker(problem)
    client.handle({"op": "inject", "problem_id": "toy"})
    client.handle({"op": "recover"})
    try:
        first = client.handle({"op": "refresh"})
    finally:
        done.set()
    assert not first["success"]
    assert first["status"] == "timeout"
    client.handle({"op": "recover"})
    with pytest.raises(RuntimeError, match="recovery"):
        client.handle({"op": "inject", "problem_id": "next"})
    assert client.handle({"op": "refresh"})["status"] == "timeout"
    with pytest.raises(RuntimeError, match="recovery"):
        client.handle({"op": "inject", "problem_id": "next"})
