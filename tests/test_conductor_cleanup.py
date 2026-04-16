"""Tests for deferred cleanup: Conductor gate state, force_cleanup concurrency,
watchdog timer, noise-restart guard, and the POST /cleanup endpoint.

Background: Crucible and other reflection-capable agents opt in via
``defer_cleanup: true`` in ``agents.yaml``. When set, Conductor holds teardown
(recover_fault + undeploy + reconcile) until the agent signals completion by
POSTing /cleanup; the driver's crash-path and a watchdog timer are safety nets.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sregym.conductor import conductor_api
from sregym.conductor.conductor_api import app


def _make_conductor_like(defer_cleanup: bool):
    """Mint a bare-bones object with just the fields/methods the endpoints touch.

    We avoid constructing the real Conductor (requires kubeconfig, helm, etc.)
    and instead exercise the state transitions directly at the endpoint layer.
    """
    obj = MagicMock()
    obj.submission_stage = "diagnosis"
    obj._defer_cleanup = defer_cleanup
    return obj


# ---------------------------------------------------------------------------
# POST /cleanup endpoint
# ---------------------------------------------------------------------------


class TestCleanupEndpoint:
    def test_no_conductor_returns_400(self, monkeypatch):
        monkeypatch.setattr(conductor_api, "_conductor", None)
        client = TestClient(app)
        resp = client.post("/cleanup")
        assert resp.status_code == 400

    def test_cleanup_during_diagnosis_rejected(self, monkeypatch):
        fake = _make_conductor_like(defer_cleanup=True)
        fake.submission_stage = "diagnosis"
        monkeypatch.setattr(conductor_api, "_conductor", fake)
        client = TestClient(app)
        resp = client.post("/cleanup")
        assert resp.status_code == 409
        fake.force_cleanup.assert_not_called()

    def test_cleanup_during_mitigation_rejected(self, monkeypatch):
        fake = _make_conductor_like(defer_cleanup=True)
        fake.submission_stage = "mitigation"
        monkeypatch.setattr(conductor_api, "_conductor", fake)
        client = TestClient(app)
        resp = client.post("/cleanup")
        assert resp.status_code == 409

    def test_cleanup_when_done_is_noop(self, monkeypatch):
        fake = _make_conductor_like(defer_cleanup=True)
        fake.submission_stage = "done"
        monkeypatch.setattr(conductor_api, "_conductor", fake)
        client = TestClient(app)
        resp = client.post("/cleanup")
        assert resp.status_code == 200
        assert resp.json()["status"] == "noop"
        fake.force_cleanup.assert_not_called()

    def test_cleanup_awaiting_runs_force_cleanup(self, monkeypatch):
        fake = _make_conductor_like(defer_cleanup=True)
        fake.submission_stage = "awaiting_cleanup"

        def _force():
            fake.submission_stage = "done"

        fake.force_cleanup.side_effect = _force
        monkeypatch.setattr(conductor_api, "_conductor", fake)
        client = TestClient(app)
        resp = client.post("/cleanup")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert resp.json()["stage"] == "done"
        fake.force_cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# Conductor state transitions + concurrency
#
# We stub out side-effecting methods (_run_teardown, noise manager, kubectl)
# so we can exercise the gate logic without a real cluster.
# ---------------------------------------------------------------------------


def _patched_conductor(defer_cleanup: bool):
    """Construct a Conductor with all external dependencies patched out so we
    only exercise the gate/teardown state machine."""
    from sregym.conductor import conductor as conductor_mod

    # Avoid running the heavy __init__ (kubeconfig, proxy, registries).
    c = conductor_mod.Conductor.__new__(conductor_mod.Conductor)
    c._defer_cleanup = defer_cleanup
    c._cleanup_lock = threading.Lock()
    c._cleanup_timer = None
    c.submission_stage = None
    c.problem = None
    c._baseline_captured = False
    c.logger = MagicMock()
    c.undeploy_app = MagicMock()
    c.cluster_state = MagicMock()
    return c


class TestForceCleanup:
    def test_force_cleanup_runs_once_on_concurrent_calls(self, monkeypatch):
        from sregym.conductor import conductor as conductor_mod

        c = _patched_conductor(defer_cleanup=True)
        c.submission_stage = "awaiting_cleanup"

        call_count = {"n": 0}
        barrier = threading.Barrier(2)

        def slow_teardown(self):
            call_count["n"] += 1
            barrier.wait(timeout=5)
            time.sleep(0.1)

        monkeypatch.setattr(conductor_mod.Conductor, "_run_teardown", slow_teardown)

        def _call():
            c.force_cleanup()

        t1 = threading.Thread(target=_call)
        t2 = threading.Thread(target=_call)
        t1.start()
        # Give the first thread a moment to grab the lock before the second joins.
        time.sleep(0.05)
        t2.start()
        # Release the first thread.
        try:
            barrier.wait(timeout=5)
        except threading.BrokenBarrierError:
            pass
        t1.join(timeout=5)
        t2.join(timeout=5)

        # The lock guarantees exactly-one teardown: when the second caller
        # acquires the lock, stage is already "done" and it returns early.
        assert call_count["n"] == 1
        assert c.submission_stage == "done"

    def test_force_cleanup_noop_when_already_done(self):
        c = _patched_conductor(defer_cleanup=True)
        c.submission_stage = "done"
        with patch.object(type(c), "_run_teardown") as teardown_mock:
            c.force_cleanup()
        teardown_mock.assert_not_called()


class TestWatchdog:
    def test_watchdog_fires_after_timeout(self, monkeypatch):
        """If no /cleanup arrives within the configured timeout, the watchdog
        runs teardown so a wedged agent cannot indefinitely hold the cluster."""
        from sregym.conductor import conductor as conductor_mod

        # Force a fast watchdog for the test.
        monkeypatch.setenv("SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS", "0.1")

        c = _patched_conductor(defer_cleanup=True)
        c.submission_stage = "awaiting_cleanup"

        teardown_called = threading.Event()

        def marker(self):
            teardown_called.set()

        monkeypatch.setattr(conductor_mod.Conductor, "_run_teardown", marker)

        c._start_cleanup_watchdog()
        assert teardown_called.wait(timeout=3), "Watchdog did not fire within 3s"
        # Give the timer thread a moment to finish setting stage.
        for _ in range(50):
            if c.submission_stage == "done":
                break
            time.sleep(0.02)
        assert c.submission_stage == "done"

    def test_cancel_watchdog_stops_timer(self, monkeypatch):
        monkeypatch.setenv("SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS", "5")

        c = _patched_conductor(defer_cleanup=True)
        c.submission_stage = "awaiting_cleanup"
        c._start_cleanup_watchdog()
        assert c._cleanup_timer is not None

        c._cancel_cleanup_watchdog()
        assert c._cleanup_timer is None
