# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""DNSResolutionMitigationOracle against an in-memory namespace (no cluster)."""

import json
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.dns_resolution_mitigation import DNSResolutionMitigationOracle

NS = "train-ticket"


class FakeKubectl:
    """The slice of KubeCtl the oracle uses. Like KubeCtl.exec_command, a failing command
    returns its stderr instead of raising. `pods` maps a Running pod to the Service names it
    can resolve, or to None when its container cannot be exec'd (crash looping)."""

    def __init__(self, services, pods):
        self.services, self.pods = services, pods
        self.core_v1_api = SimpleNamespace(read_namespaced_pod=lambda name, namespace: name)

    def list_services(self, namespace):
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=s)) for s in self.services])

    def exec_command(self, command):
        if command.startswith("kubectl get service "):
            return json.dumps({"app": command.split()[3]})
        if command.startswith("kubectl get pods "):
            return " ".join(self.pods)
        raise AssertionError(command)


def _evaluate(services, pods):
    problem = SimpleNamespace(kubectl=FakeKubectl(services, pods), namespace=NS, faulty_service="ts-user-service")
    oracle = DNSResolutionMitigationOracle(problem)
    oracle._probe = lambda pod, services: pods[pod] is not None and set(services) <= pods[pod]
    return oracle.evaluate()["success"]


SERVICES = ["ts-user-service", "ts-auth-service", "nacos"]


def test_passes_when_every_pod_resolves_every_service():
    assert _evaluate(SERVICES, {"old": set(SERVICES), "new": set(SERVICES)})


@pytest.mark.parametrize(
    "pods",
    [
        # Regression (live #150): a stalled rollout leaves the old pod resolving while the
        # faulty pod crash loops; checking only the first pod passed with the fault in place.
        {"old": set(SERVICES), "new": None},
        {"old": set(SERVICES), "new": {"ts-user-service"}},
        {"only": None},
        {},
    ],
)
def test_fails_when_any_running_pod_cannot_resolve(pods):
    assert not _evaluate(SERVICES, pods)
