from types import SimpleNamespace

import pytest

import sregym.conductor.oracles.data_plane_progress as dppo
from sregym.conductor.oracles.data_plane_progress import DataPlaneProgressOracle

POISON = 20


class _Cluster:
    """The archiver's audit log as a sequence of snapshots, one per read, and the consumer pods."""

    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.deleted = []

    def exec_command(self, command):
        if command.startswith("kubectl delete pod"):
            self.deleted.append(command.split()[3])
            return ""
        assert command.startswith("kubectl logs deployment/orders-archiver -n astronomy-shop")
        offsets = self.snapshots.pop(0) if len(self.snapshots) > 1 else self.snapshots[0]
        return "\n".join(f"INFO AUDIT src_offset={o}" for o in offsets)

    def list_pods(self, namespace):
        pod = SimpleNamespace(
            metadata=SimpleNamespace(name="orders-validator-abc", labels={"app": "orders-validator"}),
            status=SimpleNamespace(phase="Running"),
        )
        return SimpleNamespace(items=[pod])


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(dppo.time, "sleep", lambda _s: None)


def _oracle(snapshots):
    cluster = _Cluster(snapshots)
    problem = SimpleNamespace(poison_offset=POISON, kubectl=cluster, namespace="astronomy-shop")
    oracle = DataPlaneProgressOracle(
        problem,
        consumer_group="orders-validator",
        topic="orders-fulfillment",
        consumer_deployment="orders-validator",
        progress_timeout=30,
        restart_recover_timeout=30,
    )
    return oracle, cluster


def _through(high, skip=()):
    return [o for o in range(high + 1) if o != POISON and o not in skip]


def test_a_drained_pipeline_that_survives_a_consumer_restart_passes():
    oracle, cluster = _oracle([_through(22), _through(24), _through(24), _through(26)])

    assert oracle.evaluate() == {"success": True}
    assert cluster.deleted == ["orders-validator-abc"]


def test_a_consumer_still_stalled_at_the_poison_record_fails():
    oracle, _ = _oracle([_through(POISON - 1)])

    assert oracle.evaluate() == {"success": False, "reason": "offset not advanced past poison record"}


def test_skipping_valid_records_behind_the_poison_is_data_loss():
    oracle, _ = _oracle([_through(25, skip={21, 22})])

    assert oracle.evaluate() == {"success": False, "reason": "data loss: valid records skipped"}


def test_progress_that_stops_after_the_skip_fails():
    oracle, _ = _oracle([_through(22)])

    assert oracle.evaluate() == {"success": False, "reason": "no forward progress"}


def test_unknown_poison_offset_fails_without_reading_the_cluster():
    oracle, _ = _oracle([])
    oracle.problem.poison_offset = None

    assert oracle.evaluate()["success"] is False
