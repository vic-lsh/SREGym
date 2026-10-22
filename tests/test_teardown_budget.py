"""LocalPV cleanup has one elapsed-time budget across all nodes."""

from types import SimpleNamespace

from sregym.service import kubectl


def _gc(monkeypatch, durations):
    clock = [0.0]
    calls = []
    ctl = kubectl.KubeCtl.__new__(kubectl.KubeCtl)
    ctl.core_v1_api = SimpleNamespace(list_persistent_volume=lambda: SimpleNamespace(items=[]))
    ctl.list_nodes = lambda: SimpleNamespace(
        items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in durations]
    )

    def run(*, node_name, timeout, **kwargs):
        calls.append((node_name, timeout))
        clock[0] += min(durations[node_name], timeout)
        if durations[node_name] >= timeout:
            raise TimeoutError("node did not finish")
        return 2

    ctl._run_localpv_gc_pod_on_node = run
    monkeypatch.setattr(kubectl.time, "monotonic", lambda: clock[0])
    result = ctl.gc_orphan_localpv_dirs(timeout=180)
    return result, calls, clock[0]


def test_stalled_gc_does_not_wait_a_fresh_timeout_per_node(monkeypatch):
    result, calls, elapsed = _gc(monkeypatch, {"control-plane": 180, "worker": 180})
    assert elapsed == 180
    assert calls == [("control-plane", 180)]
    assert result == {"control-plane": -1, "worker": -1}


def test_gc_shares_remaining_budget_after_a_successful_node(monkeypatch):
    result, calls, elapsed = _gc(monkeypatch, {"control-plane": 10, "worker": 180})
    assert elapsed == 180
    assert calls == [("control-plane", 180), ("worker", 170)]
    assert result == {"control-plane": 2, "worker": -1}
