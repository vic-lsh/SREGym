from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException

from sregym.conductor.oracles import cronjob_sidecar_mitigation
from sregym.conductor.oracles.cronjob_sidecar_mitigation import (
    CronJobSidecarBlocksCompletionMitigationOracle,
)
from sregym.conductor.problems.cronjob_sidecar_blocks_completion import (
    CronJobSidecarBlocksCompletionHotelReservation,
)


def _container(name, restart_policy=None):
    return SimpleNamespace(name=name, restart_policy=restart_policy)


def _cronjob(*, native_sidecar=True, other_native=False, deadline=None):
    containers = [_container("archiver")]
    init_containers = []
    if native_sidecar:
        init_containers.append(_container("fluent-bit-sidecar", "Always"))
    else:
        containers.append(_container("fluent-bit-sidecar"))
    if other_native:
        init_containers.append(_container("telemetry-bootstrap", "Always"))
    pod_spec = SimpleNamespace(containers=containers, init_containers=init_containers)
    job_spec = SimpleNamespace(active_deadline_seconds=deadline, template=SimpleNamespace(spec=pod_spec))
    return SimpleNamespace(spec=SimpleNamespace(job_template=SimpleNamespace(spec=job_spec)))


def _job(*, active=0, succeeded=0, owner="audit-log-archiver"):
    return SimpleNamespace(
        metadata=SimpleNamespace(owner_references=[SimpleNamespace(kind="CronJob", name=owner)]),
        status=SimpleNamespace(active=active, succeeded=succeeded),
    )


def _deployment(*, replicas=1, ready=1):
    return SimpleNamespace(
        metadata=SimpleNamespace(name="frontend"),
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready),
    )


class _BatchV1:
    def __init__(self, cronjob, jobs):
        self.cronjob = cronjob
        self.jobs = jobs

    def read_namespaced_cron_job(self, name, namespace):
        if self.cronjob is None:
            raise ApiException(status=404)
        return self.cronjob

    def list_namespaced_job(self, namespace):
        return SimpleNamespace(items=self.jobs)


def _evaluate(monkeypatch, *, cronjob=..., jobs=None, deployments=None):
    monkeypatch.setattr(cronjob_sidecar_mitigation, "_BEHAVIOR_PROOF_TIMEOUT_S", 0.05)
    monkeypatch.setattr(cronjob_sidecar_mitigation, "_BEHAVIOR_PROOF_POLL_INTERVAL_S", 0.01)
    problem = SimpleNamespace(namespace="hotel-reservation", cronjob_name="audit-log-archiver")
    oracle = CronJobSidecarBlocksCompletionMitigationOracle.__new__(CronJobSidecarBlocksCompletionMitigationOracle)
    oracle.problem = problem
    oracle.namespace_settled = True
    oracle.batch_v1 = _BatchV1(_cronjob() if cronjob is ... else cronjob, [_job(succeeded=1)] if jobs is None else jobs)
    deployments = [_deployment()] if deployments is None else deployments
    oracle.apps_v1 = SimpleNamespace(list_namespaced_deployment=lambda namespace: SimpleNamespace(items=deployments))
    return oracle.evaluate()["success"]


def test_passes_with_native_sidecar_bounded_jobs_healthy_app_and_a_completed_job(monkeypatch):
    jobs = [_job(active=1), _job(active=1), _job(succeeded=1)]
    assert _evaluate(monkeypatch, jobs=jobs) is True


def test_any_restartable_init_container_counts_as_the_native_sidecar_fix(monkeypatch):
    assert _evaluate(monkeypatch, cronjob=_cronjob(native_sidecar=False, other_native=True)) is True


@pytest.mark.parametrize(
    "cronjob",
    [None, _cronjob(native_sidecar=False), _cronjob(native_sidecar=False, deadline=30)],
    ids=["deleted", "unfixed", "deadline-workaround"],
)
def test_rejects_a_cronjob_without_the_native_sidecar_pattern(monkeypatch, cronjob):
    assert _evaluate(monkeypatch, cronjob=cronjob) is False


def test_rejects_accumulated_active_jobs(monkeypatch):
    jobs = [_job(active=1) for _ in range(3)] + [_job(succeeded=1), _job(active=1, owner="other")]
    assert _evaluate(monkeypatch, jobs=jobs) is False


def test_rejects_collateral_damage_to_the_application(monkeypatch):
    assert _evaluate(monkeypatch, deployments=[_deployment(replicas=2, ready=1)]) is False


def test_rejects_when_no_owned_job_ever_completes(monkeypatch):
    assert _evaluate(monkeypatch, jobs=[_job(active=1), _job(succeeded=1, owner="other")]) is False


def test_native_recovery_moves_the_sidecar_to_a_restartable_init_container():
    problem = object.__new__(CronJobSidecarBlocksCompletionHotelReservation)
    problem.namespace = "hotel-reservation"

    broken = problem._build_cronjob_body()
    broken_pod = broken["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert [container["name"] for container in broken_pod["containers"]] == ["archiver", "fluent-bit-sidecar"]

    recovered = problem._build_cronjob_body_with_native_sidecar()
    pod_spec = recovered["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert [container["name"] for container in pod_spec["containers"]] == ["archiver"]
    assert pod_spec["initContainers"][0]["name"] == "fluent-bit-sidecar"
    assert pod_spec["initContainers"][0]["restartPolicy"] == "Always"
