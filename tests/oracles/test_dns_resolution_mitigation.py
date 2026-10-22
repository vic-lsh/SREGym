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
import re
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.dns_resolution_mitigation import DNSResolutionMitigationOracle, resolved

NS = "train-ticket"


class FakeKubectl:
    """The slice of KubeCtl the oracle uses. Like KubeCtl.exec_command, a failing command
    returns its stderr instead of raising. `pods` maps a Running pod to the Service names it
    can resolve, or to None when its container cannot be exec'd (crash looping)."""

    def __init__(self, services, pods):
        self.services, self.pods = services, pods

    def list_services(self, namespace):
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=s)) for s in self.services])

    def exec_command(self, command):
        if command.startswith("kubectl get service "):
            return json.dumps({"app": command.split()[3]})
        if command.startswith("kubectl get pods "):
            return " ".join(self.pods)
        m = re.fullmatch(rf"kubectl exec -n {NS} (\S+) -- getent hosts (\S+)\.{NS}\.svc\.cluster\.local", command)
        assert m, command
        pod, svc = m.groups()
        if pod not in self.pods:
            return f'Error from server (NotFound): pods "{pod}" not found'
        if self.pods[pod] is None:
            return f'error: Internal error occurred: unable to upgrade connection: container not found ("{pod}")'
        if svc in self.pods[pod]:
            return f"10.96.0.7       {svc}.{NS}.svc.cluster.local\n"
        return "command terminated with exit code 2"


def _evaluate(services, pods):
    problem = SimpleNamespace(kubectl=FakeKubectl(services, pods), namespace=NS, faulty_service="ts-user-service")
    return DNSResolutionMitigationOracle(problem).evaluate()["success"]


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


def test_resolved_requires_the_name_in_a_hosts_line():
    fqdn = f"nacos.{NS}.svc.cluster.local"
    assert resolved(f"10.96.0.9   {fqdn}\n", fqdn)
    for output in (
        "",
        "command terminated with exit code 2",
        'unable to upgrade connection: container not found ("x")',
    ):
        assert not resolved(output, fqdn)
