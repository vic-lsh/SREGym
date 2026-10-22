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
"""AlertOracle verdicts are attributable to one fault in a composite; a single fault is unchanged.

Alert payloads have the label sets the rules in ``observer/prometheus/prometheus/values.yaml``
produce (their ``by (...)`` clauses); the alert names per fault are those of
``tests/alerts/expected_alerts.yaml``. The unrelated alerts are the ones the vanilla pilot
(2026-10-22) logged: ``ContainerOOMRisk`` on an ``accounting`` pod (slot 4, which failed three
flagd faults) and a Locust ``HighRequestErrorRate`` with no workload label (slot 1).
"""

import random

import pytest

import sregym.conductor.problems.composite as composite_module
from sregym.conductor.oracles.alert_attribution import FLAG_SCOPES, AlertScope, alert_workload, fault_scope
from sregym.conductor.oracles.alert_oracle import AlertOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composite import ComponentSpec, CompositeFaultProblem, CompositeSpec
from sregym.conductor.problems.composition.catalog import otel_flags

NS = "astronomy-shop"


def _alert(alertname, severity="critical", state="firing", **labels):
    return {"labels": {"alertname": alertname, "namespace": NS, "severity": severity, **labels}, "state": state}


def _pod_alert(name, pod, severity="warning"):  # by (namespace, pod, container)
    return _alert(name, severity, pod=pod, container=pod.split("-")[0])


def _span_alert(name, service):  # by (service_name, namespace)
    return _alert(name, service_name=service)


def _locust_error_rate(endpoint):  # locust_requests_* series: no workload label
    return _alert("HighRequestErrorRate", method="GET", name=endpoint)


# Slot 4's unrelated warning, as the oracle printed it: ContainerOOMRisk (accounting-67565dc799-5tcv5).
ACCOUNTING_OOM = _pod_alert("ContainerOOMRisk", "accounting-67565dc799-5tcv5")

# Alerts each flagd fault raises while injected (expected_alerts.yaml), at its own workloads.
FAULT_ALERTS = {
    "adFailure": [_span_alert("HighRequestErrorRate", "ad")],
    "adHighCpu": [_pod_alert("ContainerCPUThrottling", "ad-5d8f7c9b4-x2k7q"), _span_alert("HighRequestLatency", "ad")],
    "adManualGc": [_span_alert("HighRequestLatency", "ad")],
    "imageSlowLoad": [_span_alert("HighRequestLatency", "frontend")],
    "cartFailure": [_span_alert("HighRequestErrorRate", "cart")],
    "failedReadinessProbe": [
        _pod_alert("KubePodNotReady", "cart-6b7d9f5c8-q4m2n", "critical"),
        _alert("DeploymentNotReady", deployment="cart"),
        _locust_error_rate("/api/cart"),
    ],
    "paymentFailure": [_span_alert("HighRequestErrorRate", "payment")],
    "paymentUnreachable": [_span_alert("HighRequestErrorRate", "checkout")],
    "productCatalogFailure": [_span_alert("HighRequestErrorRate", "product-catalog")],
    "kafkaQueueProblems": [
        _alert("DeploymentNotReady", deployment="kafka"),
        _pod_alert("HighPodRestartRate", "fraud-detection-7c9d8b6f5-z8r4t"),
        _alert("MessageConsumerLag", "warning"),
    ],
    "loadGeneratorFloodHomepage": [_span_alert("HighRequestRate", "frontend")],
}

# Flags whose only alerts name a workload: a subject-less Locust error alert is not theirs.
_LATENCY_OR_RATE_ONLY = {"adHighCpu", "adManualGc", "imageSlowLoad", "loadGeneratorFloodHomepage"}

# The span-metrics alert each fault shows on the services that call it (expected_alerts.yaml).
CALLER_SYMPTOM = {
    "adHighCpu": "HighRequestLatency",
    "adManualGc": "HighRequestLatency",
    "imageSlowLoad": "HighRequestLatency",
    "loadGeneratorFloodHomepage": "HighRequestRate",
}

# Each fault's call path, read off the opentelemetry-demo chart's *_ADDR / *_HOST env vars by
# hand (stated here, not derived from alert_attribution.CALLS, so a wrong edge there fails):
# the fault's own services, then every service whose requests reach them.
CALL_PATH = {
    "adFailure": ({"ad"}, {"frontend", "frontend-proxy", "load-generator"}),
    "adHighCpu": ({"ad"}, {"frontend", "frontend-proxy", "load-generator"}),
    "adManualGc": ({"ad"}, {"frontend", "frontend-proxy", "load-generator"}),
    "cartFailure": ({"cart"}, {"checkout", "frontend", "frontend-proxy", "load-generator"}),
    "failedReadinessProbe": ({"cart"}, {"checkout", "frontend", "frontend-proxy", "load-generator"}),
    "imageSlowLoad": ({"frontend", "frontend-proxy", "image-provider"}, {"load-generator"}),
    "kafkaQueueProblems": ({"kafka", "checkout", "fraud-detection"}, {"frontend", "frontend-proxy", "load-generator"}),
    "loadGeneratorFloodHomepage": ({"frontend", "frontend-proxy", "load-generator"}, set()),
    "paymentFailure": ({"payment"}, {"checkout", "frontend", "frontend-proxy", "load-generator"}),
    "paymentUnreachable": ({"checkout", "payment"}, {"frontend", "frontend-proxy", "load-generator"}),
    "productCatalogFailure": (
        {"product-catalog"},
        {"checkout", "frontend", "frontend-proxy", "load-generator", "recommendation", "product-reviews"},
    ),
}

# Every astronomy-shop service that emits span metrics or runs as a workload.
SERVICES = {
    "accounting", "ad", "cart", "checkout", "currency", "email", "flagd", "fraud-detection", "frontend",
    "frontend-proxy", "image-provider", "kafka", "llm", "load-generator", "payment", "product-catalog",
    "product-reviews", "quote", "recommendation", "shipping", "valkey-cart",
}  # fmt: skip


def _otel_flags():
    """Feature flag of every astronomy_shop flagd entry in the otel_flags catalog, from its footprint."""
    prefix = "data.demo.flagd.json.flags."
    flags = {}
    for entry in otel_flags.ENTRIES:
        if "astronomy_shop" not in entry.apps:
            continue
        draw = entry.sample("astronomy_shop", random.Random(0))
        for fp in draw.footprints:
            if (fp.field or "").startswith(prefix):
                flags[entry.key] = fp.field[len(prefix) :].split(".")[0]
    return flags


class Clock:
    """Fake monotonic clock: sleep advances it at once."""

    def __init__(self):
        self.now = 0.0
        self.polls = 0

    def sleep(self, seconds):
        self.now += seconds


class FlagFault(Problem):
    def __init__(self, flag, payload, faulty_service=None):
        super().__init__(
            app=type("App", (), {"name": "OpenTelemetry Demo Astronomy Shop", "namespace": NS})(), namespace=NS
        )
        self.feature_flag = flag
        if faulty_service:
            self.faulty_service = faulty_service
        self.clock = Clock()

        def fetch():
            self.clock.polls += 1
            return {"status": "success", "data": {"alerts": list(payload)}}

        self.mitigation_oracle = AlertOracle(
            self, fetch_alerts=fetch, clock=lambda: self.clock.now, sleep=self.clock.sleep
        )

    def inject_fault(self):
        pass

    def recover_fault(self):
        pass


class _Judge:
    def __init__(self, problem, expected):
        self.problem, self.expected = problem, expected


@pytest.fixture(autouse=True)
def _offline_judge(monkeypatch):
    monkeypatch.setattr(composite_module, "LLMAsAJudgeOracle", _Judge)


def _verdict(fault):
    return fault.mitigation_oracle.evaluate()["success"]


def _composite(*faults):
    """Build the composite only for its construction-time effect: scoping each fault's oracle."""
    by_name = {f"fault{i}": fault for i, fault in enumerate(faults)}
    spec = CompositeSpec(
        problem_id="composite_alert_attribution_astronomy_shop",
        components=tuple(ComponentSpec("fault", problem=name) for name in by_name),
    )
    return CompositeFaultProblem(spec, component_factory=by_name.__getitem__)


def test_every_otel_flags_flagd_entry_has_a_scope_and_recorded_alerts():
    flags = _otel_flags()
    assert set(flags.values()) == set(FLAG_SCOPES) == set(FAULT_ALERTS), flags


@pytest.mark.parametrize("flag", sorted(FLAG_SCOPES))
def test_null_agent_fails_in_a_composite(flag):
    fault = FlagFault(flag, [*FAULT_ALERTS[flag], ACCOUNTING_OOM])
    other = FlagFault("adFailure" if flag != "adFailure" else "cartFailure", [])
    _composite(fault, other)
    assert _verdict(fault) is False


@pytest.mark.parametrize("flag", sorted(FLAG_SCOPES))
def test_perfect_agent_passes_in_a_composite_despite_an_unrelated_warning(flag):
    unrelated = [ACCOUNTING_OOM]
    if "frontend-proxy" not in FLAG_SCOPES[flag].workloads:  # slot 1's throttling warning
        unrelated.append(_pod_alert("ContainerCPUThrottling", "frontend-proxy-6c9669bb69-t9r6c"))
    fault = FlagFault(flag, unrelated)
    _composite(fault, FlagFault("productCatalogFailure", []))
    assert _verdict(fault) is True
    # The whole 30 s buffer + 120 s silence window was observed, polling throughout.
    assert fault.clock.now >= 150 and fault.clock.polls >= 12


@pytest.mark.parametrize("flag", sorted(FLAG_SCOPES))
def test_single_fault_judges_the_whole_namespace_as_before(flag):
    assert _verdict(FlagFault(flag, FAULT_ALERTS[flag])) is False
    assert _verdict(FlagFault(flag, [ACCOUNTING_OOM])) is False
    assert _verdict(FlagFault(flag, [])) is True


@pytest.mark.parametrize("flag", sorted(FLAG_SCOPES))
def test_another_faults_workload_alerts_off_this_call_path_do_not_fail_this_fault(flag):
    own, path_callers = CALL_PATH[flag]
    others = [a for f, alerts in FAULT_ALERTS.items() for a in alerts if f != flag]
    off_path = [a for a in others if alert_workload(a["labels"]) not in own | path_callers | {None}]
    assert off_path or flag == "loadGeneratorFloodHomepage"
    fault = FlagFault(flag, off_path)
    _composite(fault, FlagFault("adFailure", []))
    assert _verdict(fault) is True


def _caller_cases():
    # Not load-generator: AlertOracle drops its alerts in any scope (an infra source).
    return [(flag, c) for flag in sorted(CALL_PATH) for c in sorted(CALL_PATH[flag][1]) if c != "load-generator"]


@pytest.mark.parametrize(("flag", "caller"), _caller_cases())
def test_the_faults_symptom_on_a_caller_fails_it(flag, caller):
    """paymentFailure fires HighRequestErrorRate on checkout and frontend, not only on payment."""
    fault = FlagFault(flag, [_span_alert(CALLER_SYMPTOM.get(flag, "HighRequestErrorRate"), caller)])
    _composite(fault, FlagFault("adFailure" if flag != "adFailure" else "cartFailure", []))
    assert _verdict(fault) is False


@pytest.mark.parametrize("flag", sorted(CALL_PATH))
def test_the_same_alert_on_a_service_off_the_call_path_does_not_fail_it(flag):
    own, path_callers = CALL_PATH[flag]
    unrelated = sorted(SERVICES - own - path_callers)
    assert unrelated
    alerts = [_span_alert(CALLER_SYMPTOM.get(flag, "HighRequestErrorRate"), s) for s in unrelated]
    fault = FlagFault(flag, alerts)
    _composite(fault, FlagFault("adFailure" if flag != "adFailure" else "cartFailure", []))
    assert _verdict(fault) is True


@pytest.mark.parametrize("flag", sorted(CALL_PATH))
def test_a_resource_alert_on_a_caller_does_not_fail_it(flag):
    """Callers inherit the fault's request symptoms, not their own CPU or memory warnings."""
    _, path_callers = CALL_PATH[flag]
    alerts = [_pod_alert("ContainerCPUThrottling", f"{c}-6c9669bb69-t9r6c") for c in sorted(path_callers)]
    fault = FlagFault(flag, alerts)
    _composite(fault, FlagFault("adFailure" if flag != "adFailure" else "cartFailure", []))
    assert _verdict(fault) is True


def test_the_scopes_match_the_stated_call_paths():
    for flag, (own, path_callers) in CALL_PATH.items():
        scope = FLAG_SCOPES[flag]
        assert (scope.workloads, scope.callers) == (own, path_callers), flag


def test_subjectless_alert_fails_every_fault_that_raises_its_name_and_no_other():
    # A Locust error alert cannot be pinned to one fault: it never credits an error-rate fault.
    locust = _locust_error_rate("/api/checkout")
    for flag in FLAG_SCOPES:
        fault = FlagFault(flag, [locust])
        _composite(fault, FlagFault("adFailure", []))
        assert _verdict(fault) is (flag in _LATENCY_OR_RATE_ONLY), flag


def test_resolved_and_pending_alerts_never_count():
    fault = FlagFault("adFailure", [{**FAULT_ALERTS["adFailure"][0], "state": "pending"}])
    _composite(fault, FlagFault("cartFailure", []))
    assert _verdict(fault) is True


def test_variant_service_widens_the_flag_scope_and_its_callers():
    scope = fault_scope(FlagFault("adManualGc", [], faulty_service="recommendation"))
    assert scope.workloads == {"ad", "recommendation"}
    assert scope.callers == {"frontend", "frontend-proxy", "load-generator"}
    scope = fault_scope(FlagFault("adFailure", [], faulty_service="quote"))
    assert scope.callers == {"shipping", "checkout", "frontend", "frontend-proxy", "load-generator"}


def test_faults_without_a_known_flag_stay_namespace_wide():
    assert fault_scope(FlagFault("someFutureFlag", [], faulty_service="ad")) is None
    fault = FlagFault("someFutureFlag", [ACCOUNTING_OOM])
    _composite(fault, FlagFault("adFailure", []))
    assert _verdict(fault) is False


@pytest.mark.parametrize(
    ("labels", "workload"),
    [
        ({"pod": "accounting-67565dc799-5tcv5"}, "accounting"),
        ({"pod": "frontend-proxy-6c9669bb69-t9r6c"}, "frontend-proxy"),
        ({"pod": "kafka-0"}, "kafka"),
        ({"pod": "valkey-cart"}, "valkey-cart"),
        ({"deployment": "cart", "pod": "x-1"}, "cart"),
        ({"service_name": "product-catalog"}, "product-catalog"),
        ({"service": "prometheus-kube-state-metrics", "pod": "ad-5d8f7c9b4-x2k7q"}, "ad"),
        ({"method": "GET", "name": "/api/cart"}, None),
    ],
)
def test_alert_workload(labels, workload):
    assert alert_workload(labels) == workload


def test_frontend_pod_is_not_mistaken_for_frontend_proxy():
    scope = AlertScope(frozenset({"frontend"}))
    assert not scope.attributes({"alertname": "ContainerCPUThrottling", "pod": "frontend-proxy-6c9669bb69-t9r6c"})
    assert scope.attributes({"alertname": "ContainerCPUThrottling", "pod": "frontend-7f9c8d6b5-abcde"})
