"""A fixture reset can create its real traffic Job without racing writers."""
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException

from sregym.generators.workload import wrk2


@pytest.mark.parametrize("suspended", [False, True])
def test_initial_suspension_is_part_of_job_creation(monkeypatch, suspended):
    jobs = []

    class API:
        def read_namespaced_job(self, **kwargs):
            raise ApiException(status=404)

        def create_namespaced_job(self, *, namespace, body):
            assert namespace == "sregym"
            jobs.append(body)
            return SimpleNamespace(metadata=SimpleNamespace(name=body["metadata"]["name"]))

    monkeypatch.setattr(wrk2.client, "BatchV1Api", API)
    generator = wrk2.Wrk2.__new__(wrk2.Wrk2)
    generator.namespace = "sregym"
    generator.create_wrk_job("wrk2-job", "sregym", "workload.lua", suspended=suspended)
    assert len(jobs) == 1
    assert jobs[0]["spec"]["suspend"] is suspended
    assert jobs[0]["spec"]["template"]["spec"]["containers"][0]["args"] == [
        "/bin/bash", "/scripts/wrk2-workload.sh"
    ]
