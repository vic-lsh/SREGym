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
"""Which firing Prometheus alerts are attributable to one fault (``AlertOracle`` in a composite).

A single-fault ``AlertOracle`` fails on any alert in the namespace. In a composite of N faults
that makes every alert-graded fault fail on one unrelated warning (a ``ContainerOOMRisk`` on
``accounting`` failed three flagd faults whose flags the agent had turned off). ``AlertScope``
narrows an oracle to the alerts its own fault can explain:

* An alert whose labels name a workload (``deployment``, ``statefulset``, ``service_name``, or a
  ``pod`` reduced to its owner's name) is attributable if that workload is one of the fault's.
  Any alert name counts there: an alert on the fault's own workload is presumed to be the fault's.
* It is also attributable if the workload is a caller of the fault's (directly or transitively,
  per ``CALLS``) and the alert is a span-metrics alert the fault raises. Span-metrics rules group
  ``by (service_name)``, so a failing payment service shows up as errors on checkout and
  frontend under their own names; a resource alert (e.g. CPU throttling) on a caller does not.
* An alert that names no workload (the Locust ``HighRequestErrorRate``, ``MessageConsumerLag``)
  cannot be pinned to one fault, so it is attributable iff its name is one the fault is known to
  raise (all names when the fault declares none). This never credits a fix that was not made, at
  the cost that such an alert caused by one fault also fails the others that raise the same name.

Standard library only, so the composition tests import it without SREGym's dependencies.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

# Labels that name the workload an alert is about, most specific first. Not ``service``: on
# kube-state-metrics series it names the scraped exporter, not the subject.
_WORKLOAD_LABELS = ("deployment", "statefulset", "daemonset", "service_name")

# <deployment>-<replicaset hash>-<pod suffix>, <statefulset>-<ordinal>.
_DEPLOYMENT_POD = re.compile(r"^(?P<owner>.+)-[a-z0-9]{6,10}-[a-z0-9]{5}$")
_STATEFULSET_POD = re.compile(r"^(?P<owner>.+)-\d+$")

_ERROR_RATE = frozenset({"HighRequestErrorRate", "ServiceErrorRateSpike", "EndpointCompleteOutage"})
# Rules over span metrics, grouped by (service_name, namespace) (observer/prometheus/prometheus/
# values.yaml): what a fault's symptom looks like on the services that call it.
_SPAN_METRIC = _ERROR_RATE | {"HighRequestRate", "HighRequestLatency"}

# astronomy_shop's call graph, caller -> callees, from the opentelemetry-demo Helm chart SREGym
# deploys (0.40.7, demo 2.2.0; its subchart versions are the ones vendored under
# SREGym-applications/astronomy-shop/charts/opentelemetry-demo/charts): each component's
# ``*_ADDR`` / ``*_HOST`` env vars in the chart's values.yaml. load-generator targets
# frontend-proxy (LOCUST_HOST); frontend-proxy (envoy) routes to frontend and image-provider
# (and to flagd, grafana, jaeger and the Locust UI, left out: no shop request passes through
# them). accounting and fraud-detection consume from kafka and call nothing.
CALLS: dict[str, frozenset[str]] = {
    "load-generator": frozenset({"frontend-proxy"}),
    "frontend-proxy": frozenset({"frontend", "image-provider"}),
    "frontend": frozenset(
        {"ad", "cart", "checkout", "currency", "product-catalog", "product-reviews", "recommendation", "shipping"}
    ),
    "checkout": frozenset({"cart", "currency", "email", "payment", "product-catalog", "shipping", "kafka"}),
    "recommendation": frozenset({"product-catalog"}),
    "product-reviews": frozenset({"product-catalog", "llm"}),
    "shipping": frozenset({"quote"}),
    "cart": frozenset({"valkey-cart"}),
}


def callers(services: Iterable[str]) -> frozenset[str]:
    """Every service that calls one of *services*, directly or through others, excluding them."""
    services = frozenset(services)
    found: set[str] = set()
    frontier = set(services)
    while frontier:
        frontier = {c for c, callees in CALLS.items() if callees & frontier} - found - services
        found |= frontier
    return frozenset(found)


@dataclass(frozen=True)
class AlertScope:
    """The alerts attributable to one fault: any alert on ``workloads``, a span-metrics alert
    the fault raises on ``callers``, a workload-less alert named in ``alertnames``
    (``None``: every name). See the module doc."""

    workloads: frozenset[str]
    alertnames: frozenset[str] | None = None
    callers: frozenset[str] = frozenset()

    def attributes(self, labels: Mapping[str, str]) -> bool:
        workload = alert_workload(labels)
        name = labels.get("alertname")
        raised = self.alertnames is None or name in self.alertnames
        if workload is None:
            return raised
        if workload in self.workloads:
            return True
        return workload in self.callers and raised and name in _SPAN_METRIC


def alert_workload(labels: Mapping[str, str]) -> str | None:
    """The workload an alert's labels name, or None for a namespace-level alert."""
    for key in _WORKLOAD_LABELS:
        if labels.get(key):
            return labels[key]
    pod = labels.get("pod")
    if not pod:
        return None
    for pattern in (_DEPLOYMENT_POD, _STATEFULSET_POD):
        m = pattern.match(pod)
        if m:
            return m.group("owner")
    return pod


def _scope(workloads: Iterable[str], alertnames: Iterable[str]) -> AlertScope:
    workloads = frozenset(workloads)
    return AlertScope(workloads, frozenset(alertnames), callers(workloads))


# astronomy_shop flagd faults, keyed by feature flag. Workloads: the flag's consumers
# (``inject_otel._FLAG_TO_DEPLOYMENTS``) plus where its symptom is measured; their callers
# (``CALLS``) are derived. Alert names:
# ``tests/alerts/expected_alerts.yaml`` plus the stricter variants of the same signal; only
# consulted for alerts that name no workload.
FLAG_SCOPES: dict[str, AlertScope] = {
    "adFailure": _scope({"ad"}, _ERROR_RATE),
    "adHighCpu": _scope({"ad"}, {"ContainerCPUThrottling", "HighRequestLatency"}),
    "adManualGc": _scope({"ad"}, {"HighRequestLatency", "ContainerCPUThrottling"}),
    "cartFailure": _scope({"cart"}, _ERROR_RATE),
    "failedReadinessProbe": _scope(
        {"cart"}, _ERROR_RATE | {"KubePodNotReady", "DeploymentNotReady", "ServiceEndpointDown"}
    ),
    # The frontend asks frontend-proxy (envoy) to delay image-provider responses.
    "imageSlowLoad": _scope({"frontend", "frontend-proxy", "image-provider"}, {"HighRequestLatency"}),
    "kafkaQueueProblems": _scope(
        # kafka is restarted, checkout produces, fraud-detection's consumer is delayed. Not
        # accounting (also a consumer): it carries a ContainerOOMRisk warning with no fault
        # injected (pilot slot 4), which would fail this fault whatever the agent did.
        {"kafka", "checkout", "fraud-detection"},
        _ERROR_RATE
        | {
            "DeploymentNotReady",
            "HighPodRestartRate",
            "ServiceEndpointDown",
            "ContainerCPUThrottling",
            "ContainerOOMRisk",
            "KubePodNotReady",
            "MessageConsumerLag",
        },
    ),
    "loadGeneratorFloodHomepage": _scope(
        {"frontend", "frontend-proxy", "load-generator"}, {"HighRequestRate", "HighRequestLatency"}
    ),
    "paymentFailure": _scope({"payment"}, _ERROR_RATE),
    # checkout's calls to payment fail; payment itself sees no traffic.
    "paymentUnreachable": _scope({"checkout", "payment"}, _ERROR_RATE),
    "productCatalogFailure": _scope({"product-catalog"}, _ERROR_RATE),
}


def fault_scope(problem) -> AlertScope | None:
    """The alert scope of *problem*'s fault, or None (namespace-wide, as for a single fault).

    A flagd fault uses ``FLAG_SCOPES[problem.feature_flag]``, widened by ``problem.faulty_service``
    (variants may retarget it). Other faults stay namespace-wide: their symptoms (e.g. a crashed
    kubelet's pods) are not known to stay on one workload, and a scope that missed one would
    credit a fix that was not made.
    """
    known = FLAG_SCOPES.get(getattr(problem, "feature_flag", None) or "")
    if known is None:
        return None
    service = getattr(problem, "faulty_service", None)
    if not service:
        return known
    return _scope(known.workloads | {service}, known.alertnames)
