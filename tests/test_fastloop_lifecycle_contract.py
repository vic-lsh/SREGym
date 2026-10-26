"""SG2 boundary regressions: real composite lifecycles with fake platform I/O."""

import io
import json
import threading
from types import SimpleNamespace

import pytest

from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.colocated_composite import ColocatedComposite
from sregym.conductor.problems.composition.spec import FaultDraw, Footprint, Tier
from sregym.fastloop.worker import FastloopWorker, serve
from sregym.utils.decorators import mark_fault_injected

EMPTY_INVENTORY = SimpleNamespace(list_objects=frozenset, delete=lambda obj: None)


class Fault(Problem):
    def __init__(self, name, events, *, invalid=False):
        super().__init__(SimpleNamespace(namespace="hotel-reservation", name="Hotel Reservation"))
        self.root_cause = name
        self.name = name
        self.events = events
        self.invalid = invalid
        self.error = None
        self.mitigation_oracle = SimpleNamespace(evaluate=self.grade, scope_to_fault=lambda: None)

    def grade(self):
        self.events.append(("oracle", self.name))
        return {"success": False}

    def inject_fault(self):
        self.events.append(("inject", self.name))

    def injection_failure(self):
        return {"hidden_identity": "synthetic-canary"} if self.invalid else None

    @mark_fault_injected
    def recover_fault(self):
        self.events.append(("recover", self.name))
        if self.error:
            raise self.error


def composite(events):
    parts = [Fault("psa", events), Fault("quota", events), Fault("mongodb-auth", events)]
    draws = [
        FaultDraw(
            "psa", "hotel_reservation", (), (Footprint("Namespace", "hotel-reservation"),), Tier.BLOCKS_ADMISSION
        ),
        FaultDraw("quota", "hotel_reservation", (), (Footprint("ResourceQuota", "memory-limit-quota"),)),
        FaultDraw(
            "mongodb-auth", "hotel_reservation", (), (Footprint("Deployment", "mongodb-rate"),), Tier.RESTARTS_PODS
        ),
    ]
    problem = ColocatedComposite(
        parts, draws=draws, cluster=SimpleNamespace(kubectl=object(), read_specs=lambda ns: ({}, {}))
    )
    problem.inject_pause_s = problem.liveness_settle_s = 0
    return problem


def worker(factory):
    return FastloopWorker(
        problem_factory=factory,
        deployer=lambda *args: None,
        health_probe=lambda ns: {},
        cluster_factory=lambda *args: ("fake", "fake"),
        cluster_inventory=EMPTY_INVENTORY,
    )


def test_invalid_injection_is_typed_retained_for_cleanup_and_not_graded():
    events = []
    problem = Fault("bad", events, invalid=True)
    w = worker(lambda _: problem)
    replies = io.StringIO()
    serve(w, io.StringIO('{"id":1,"op":"inject","problem_id":"bad"}\n'), replies)
    reply = json.loads(replies.getvalue())
    assert reply["ok"] is True
    result = reply["result"]
    assert result["injection"] == {
        "valid": False,
        "infra_error": "injection_invalid",
        "details": {"hidden_identity": "synthetic-canary"},
    }
    with pytest.raises(RuntimeError, match="invalid"):
        w.handle({"op": "oracle"})
    with pytest.raises(RuntimeError, match="invalid"):
        w.handle({"op": "inject", "problem_id": "next", "compose": True})
    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    assert events == [("inject", "bad"), ("recover", "bad")]


def test_injection_exception_becomes_invalid_and_can_be_cleaned():
    problem = Fault("bad", [])

    def fail():
        raise RuntimeError("partially installed")

    problem.inject_fault = fail
    w = worker(lambda _: problem)
    result = w.handle({"op": "inject", "problem_id": "bad"})
    assert result["injection"]["valid"] is False
    assert "partially installed" in result["injection"]["details"]["error"]
    assert w.handle({"op": "recover"})["success"] is True


def test_colocated_enumeration_preserves_declared_identity_and_actual_order():
    events = []
    problem = composite(events)
    w = worker(lambda _: problem)
    result = w.handle({"op": "inject", "problem_id": "mf__synthetic"})
    assert result["injection"]["valid"] is True
    assert result["faults"] == [0, 1, 2]
    assert [c["component_index"] for c in result["components"]] == [0, 1, 2]
    assert [c["injection_order"] for c in result["components"]] == [2, 1, 0]
    assert [c["identity"]["key"] for c in result["components"]] == ["psa", "quota", "mongodb-auth"]
    events.clear()
    grade = w.handle({"op": "oracle", "fault": 1})
    assert grade["components"] == [result["components"][1]]
    assert events == [("oracle", "quota")]
    events.clear()
    single = w.handle({"op": "recover", "fault": 1, "mode": "full"})
    assert single["success"] and single["recovered"] == [1]
    assert events == [("recover", "quota")]
    remaining = w.handle({"op": "recover", "mode": "full"})
    assert remaining["success"] and remaining["recovered"] == [0, 2]
    assert events == [("recover", "quota"), ("recover", "psa"), ("recover", "mongodb-auth")]


def test_multiple_ids_flatten_components_and_recover_reverse_actual_order():
    events = []
    p = composite(events)
    w = worker(lambda pid: p if pid.startswith("mf__") else Fault(pid, events))
    first = w.handle({"op": "inject", "problem_id": "plain"})
    joined = w.handle({"op": "inject", "problem_id": "mf__synthetic", "compose": True})
    assert first["faults"] == [0] and joined["faults"] == [1, 2, 3]
    assert [c["injection_order"] for c in joined["components"]] == [3, 2, 1]
    events.clear()
    recovered = w.handle({"op": "recover", "mode": "full"})
    assert recovered["recovered"] == [1, 2, 3, 0]
    assert events == [("recover", "psa"), ("recover", "quota"), ("recover", "mongodb-auth"), ("recover", "plain")]


def test_partial_recovery_keeps_failed_fault_pending_blocks_injection_and_allows_retry():
    events = []
    p = composite(events)
    p.problems[1].error = RuntimeError("quota cleanup failed")
    w = worker(lambda _: p)
    w.handle({"op": "inject", "problem_id": "mf__synthetic"})
    events.clear()
    result = w.handle({"op": "recover", "mode": "full"})
    assert result["success"] is False and result["status"] == "partial"
    assert result["recovered"] == [0, 2] and result["pending"] == [1]
    assert result["errors"][0]["component_index"] == 1
    assert "quota cleanup failed" in result["errors"][0]["error"]
    assert events == [("recover", "psa"), ("recover", "quota"), ("recover", "mongodb-auth")]
    with pytest.raises(RuntimeError, match="recovery"):
        w.handle({"op": "inject", "problem_id": "next"})
    p.problems[1].error = None
    retry = w.handle({"op": "recover", "mode": "full"})
    assert retry["success"] and retry["recovered"] == [1] and retry["pending"] == []
    assert w.handle({"op": "inject", "problem_id": "next"})["injection"]["valid"]


def test_timeout_is_reported_and_lane_cannot_be_reused_even_after_thread_returns():
    events = []
    p = composite(events)
    finished = threading.Event()
    p.problems[1].recover_fault = lambda: finished.wait(2)
    p.recover_timeout_s = 0.01
    w = worker(lambda _: p)
    w.handle({"op": "inject", "problem_id": "mf__synthetic"})
    try:
        result = w.handle({"op": "recover", "mode": "full"})
        assert not result["success"] and result["pending"] == [1]
        assert result["errors"][0]["status"] == "timeout"
        assert 1 not in result["recovered"]
        with pytest.raises(RuntimeError, match="recovery|timeout"):
            w.handle({"op": "inject", "problem_id": "next"})
    finally:
        finished.set()
    retry = w.handle({"op": "recover", "mode": "full"})
    assert not retry["success"] and retry["pending"] == [1]


def test_unknown_recovery_mode_is_rejected_before_mutation():
    w = worker(lambda _: Fault("single", []))
    w.handle({"op": "inject", "problem_id": "single"})
    with pytest.raises(ValueError, match="mode"):
        w.handle({"op": "recover", "mode": "namespace_teardown"})


def test_recovering_another_component_does_not_clear_previous_failure():
    p = composite([])
    p.problems[1].error = RuntimeError("quota still broken")
    w = worker(lambda _: p)
    w.handle({"op": "inject", "problem_id": "mf__synthetic"})
    assert not w.handle({"op": "recover", "fault": 1})["success"]
    assert w.handle({"op": "recover", "fault": 0})["success"]
    with pytest.raises(RuntimeError, match="recovery"):
        w.handle({"op": "inject", "problem_id": "next"})


def test_component_without_an_oracle_never_uses_aggregate_grade():
    p = composite([])
    w = worker(lambda _: p)
    w.handle({"op": "inject", "problem_id": "mf__synthetic"})
    p.problems[0].mitigation_oracle = None
    with pytest.raises(RuntimeError, match="component.*oracle"):
        w.handle({"op": "oracle", "fault": 0})


def test_real_two_fault_mf_registry_id_returns_two_addressable_identities(monkeypatch):
    from sregym.conductor.oracles.llm_as_a_judge.judge import DiagnosisJudge
    from sregym.conductor.problems.registry import ProblemRegistry
    from sregym.service.apps.hotel_reservation import HotelReservation
    from sregym.service.kubectl import KubeCtl

    monkeypatch.setattr(KubeCtl, "__init__", lambda self: None)
    monkeypatch.setattr(KubeCtl, "is_emulated_cluster", lambda self: False)
    monkeypatch.setattr(KubeCtl, "list_deployments", lambda *args: SimpleNamespace(items=[]))
    monkeypatch.setattr(HotelReservation, "create_workload", lambda *args: None)
    monkeypatch.setattr(DiagnosisJudge, "__init__", lambda *args, **kwargs: None)
    pid = "mf__hotel_reservation__n2__s2596534647__v_"
    p = ProblemRegistry().get_problem_instance(pid)
    assert isinstance(p, ColocatedComposite)
    p.cluster = SimpleNamespace(kubectl=object(), read_specs=lambda ns: ({}, {}))
    p.inject_pause_s = p.liveness_settle_s = 0
    for part in p.problems:
        part.inject_fault = lambda: None
        part.recover_fault = lambda: None
        part.mitigation_oracle = SimpleNamespace(evaluate=lambda: {"success": False})
    w = worker(lambda _: p)
    result = w.handle({"op": "inject", "problem_id": pid})
    assert result["injection"]["valid"]
    assert result["faults"] == [0, 1]
    assert [c["identity"]["key"] for c in result["components"]] == [
        "wrong_bin_usage",
        "psa_restricted_blocks_recreation",
    ]
    assert w.handle({"op": "recover", "mode": "full"})["success"]


def test_colocated_full_recovery_resets_credentials_before_seed_and_refreshes_afterward():
    events = []
    problem = composite(events)
    problem.problems[1].recover_to_baseline = lambda: events.append(("baseline", "quota"))
    problem.problems[1].refresh_after_recovery = lambda: events.append(("refresh", "quota"))
    w = worker(lambda _: problem)
    w.handle({"op": "inject", "problem_id": "toy-composite"})
    events.clear()
    assert w.handle({"op": "recover", "mode": "full"})["success"]
    events.append(("seed", "all"))
    assert w.handle({"op": "refresh"})["success"]
    assert events == [
        ("recover", "psa"),
        ("baseline", "quota"),
        ("recover", "mongodb-auth"),
        ("seed", "all"),
        ("refresh", "quota"),
    ]
