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
"""HPAControlPlaneMitigationOracle: a metrics API outage is an infra error, never a verdict
on the agent, and never hides an unfixed fault. Over an in-memory kubectl (no cluster)."""

import json
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.hpa_control_plane_mitigation import (
    METRICS_SERVER_UNAVAILABLE,
    HPAControlPlaneMitigationOracle,
)

NS = "hotel-reservation"
SELECTOR = "io.kompose.service=frontend"
OUTAGE = (
    "Error from server (ServiceUnavailable): the server is currently unable to handle the request "
    "(get pods.metrics.k8s.io)"
)


def _pod(cpu_request):
    resources = {"requests": {"cpu": cpu_request}} if cpu_request else {}
    return {
        "metadata": {"name": "frontend-c5c4b4645-rswsb"},
        "spec": {"containers": [{"name": "hotel-reserv-frontend", "resources": resources}]},
        "status": {"phase": "Running", "containerStatuses": [{"name": "hotel-reserv-frontend", "ready": True}]},
    }


def _hpa(target="Utilization", readable=False):
    status = {
        "conditions": [
            {
                "type": "ScalingActive",
                "status": "True" if readable else "False",
                "reason": "ValidMetricFound" if readable else "FailedGetResourceMetric",
                "message": "" if readable else "unable to get metrics for resource cpu",
            }
        ]
    }
    if readable:
        status["currentMetrics"] = [
            {"type": "Resource", "resource": {"name": "cpu", "current": {"averageValue": "5m"}}}
        ]
    target_spec = {"type": target, "averageUtilization": 60} if target == "Utilization" else {"type": target}
    return {
        "metadata": {"name": "frontend-capacity"},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "frontend"},
            "minReplicas": 1,
            "maxReplicas": 5,
            "metrics": [{"type": "Resource", "resource": {"name": "cpu", "target": target_spec}}],
        },
        "status": status,
    }


def _metrics_server_objects():
    """What the conductor installs: the metrics-server Deployment and its APIService."""
    return {
        "deployment metrics-server -n kube-system": {
            "metadata": {"name": "metrics-server", "uid": "d-1", "generation": 1},
            "spec": {"replicas": 1, "template": {"spec": {"containers": [{"args": ["--kubelet-insecure-tls"]}]}}},
        },
        "apiservice v1beta1.metrics.k8s.io": {
            "metadata": {"name": "v1beta1.metrics.k8s.io", "uid": "a-1", "generation": 1},
            "spec": {"service": {"name": "metrics-server", "namespace": "kube-system"}, "group": "metrics.k8s.io"},
        },
    }


class FakeKubectl:
    """``exec_command`` for the commands the oracle runs. Like ``KubeCtl.exec_command``, a
    failing command returns its stderr. An unknown command fails the test. ``cluster``
    holds the metrics-server objects; deleting one makes its read fail with NotFound."""

    def __init__(self, pod, hpa, metrics_api_up):
        self.pod, self.hpa, self.metrics_api_up = pod, hpa, metrics_api_up
        self.cluster = _metrics_server_objects()
        self.commands = []

    def exec_command(self, command):
        self.commands.append(command)
        deployment = {
            "metadata": {"name": "frontend"},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"io.kompose.service": "frontend"}}},
            "status": {"readyReplicas": 1, "updatedReplicas": 1},
        }
        responses = {
            f"kubectl get deployment frontend -n {NS} -o json": deployment,
            f"kubectl get pods -n {NS} -l '{SELECTOR}' -o json": {"items": [self.pod]},
            f"kubectl get hpa -n {NS} -o json": {"items": [self.hpa]},
        }
        if command in responses:
            return json.dumps(responses[command])
        for ref, obj in _metrics_server_objects().items():
            if command == f"kubectl get {ref} -o json":
                if ref not in self.cluster:
                    return f'Error from server (NotFound): {ref.split()[0]} "{obj["metadata"]["name"]}" not found'
                return json.dumps(self.cluster[ref])
        if command == f"kubectl get --raw '/apis/metrics.k8s.io/v1beta1/namespaces/{NS}/pods?labelSelector={SELECTOR}'":
            return json.dumps({"kind": "PodMetricsList", "items": []}) if self.metrics_api_up else OUTAGE
        raise AssertionError(f"unexpected command: {command}")


def _evaluate(pod, hpa, metrics_api_up, agent=None, baseline=True):
    """Inject (record the baseline), let ``agent`` act on the fake cluster, then grade."""
    kubectl = FakeKubectl(pod, hpa, metrics_api_up)
    problem = SimpleNamespace(namespace=NS, kubectl=kubectl)
    # One poll, no sleep: the polling loop is not what these tests are about.
    oracle = HPAControlPlaneMitigationOracle(
        problem, deployment_name="frontend", hpa_name="frontend-capacity", timeout_seconds=0, consecutive_successes=1
    )
    if baseline:
        oracle.record_metrics_baseline()
    if agent:
        agent(kubectl.cluster)
    return oracle.evaluate()


def _scale_metrics_server_to_zero(cluster):
    dep = cluster["deployment metrics-server -n kube-system"]
    dep["spec"]["replicas"], dep["metadata"]["generation"] = 0, 2


def _delete_metrics_server(cluster):
    del cluster["deployment metrics-server -n kube-system"]


def _recreate_apiservice(cluster):
    cluster["apiservice v1beta1.metrics.k8s.io"]["metadata"]["uid"] = "a-2"


def _repoint_apiservice(cluster):  # a spec edit the API server did not count in generation
    cluster["apiservice v1beta1.metrics.k8s.io"]["spec"]["service"]["name"] = "elsewhere"


@pytest.mark.parametrize(
    "pod, hpa",
    [
        (_pod("100m"), _hpa()),  # the canonical fix: CPU requests restored
        (_pod(None), _hpa(target="AverageValue")),  # the accepted alternative: no request needed
    ],
    ids=["requests-restored", "average-value-target"],
)
def test_a_fixed_hpa_that_cannot_read_metrics_while_the_metrics_api_is_down_is_an_infra_error(pod, hpa):
    r = _evaluate(pod, hpa, metrics_api_up=False)

    assert r["success"] is False
    assert r["infra_error"] == METRICS_SERVER_UNAVAILABLE
    assert "ServiceUnavailable" in r["details"]


@pytest.mark.parametrize(
    "agent", [_scale_metrics_server_to_zero, _delete_metrics_server, _recreate_apiservice, _repoint_apiservice]
)
def test_an_outage_after_the_agent_changed_metrics_server_is_its_collateral_not_an_infra_error(agent):
    r = _evaluate(_pod("100m"), _hpa(), metrics_api_up=False, agent=agent)

    assert r["success"] is False and "infra_error" not in r
    assert "changed since injection" in r["details"]


def test_an_outage_with_metrics_server_status_changes_only_is_still_an_infra_error():
    def outage(cluster):  # what an outage changes: status, never uid, generation or spec
        cluster["deployment metrics-server -n kube-system"]["status"] = {"availableReplicas": 0}

    r = _evaluate(_pod("100m"), _hpa(), metrics_api_up=False, agent=outage)

    assert r["infra_error"] == METRICS_SERVER_UNAVAILABLE


def test_without_an_injection_baseline_an_outage_is_never_an_infra_error():
    r = _evaluate(_pod("100m"), _hpa(), metrics_api_up=False, baseline=False)

    assert r["success"] is False and "infra_error" not in r


def test_an_unfixed_fault_still_fails_when_the_metrics_api_is_also_down():
    r = _evaluate(_pod(None), _hpa(), metrics_api_up=False)

    assert r["success"] is False and "infra_error" not in r


def test_a_fixed_hpa_without_metrics_while_the_metrics_api_answers_is_a_plain_failure():
    r = _evaluate(_pod("100m"), _hpa(), metrics_api_up=True)

    assert r["success"] is False and "infra_error" not in r


def test_a_healthy_hpa_passes_without_asking_the_metrics_api():
    pod, hpa = _pod("100m"), _hpa(readable=True)
    kubectl = FakeKubectl(pod, hpa, metrics_api_up=False)
    oracle = HPAControlPlaneMitigationOracle(
        SimpleNamespace(namespace=NS, kubectl=kubectl),
        deployment_name="frontend",
        hpa_name="frontend-capacity",
        timeout_seconds=0,
        consecutive_successes=1,
    )

    r = oracle.evaluate()

    assert r["success"] is True and "infra_error" not in r
    assert not any("metrics.k8s.io" in c for c in kubectl.commands)
