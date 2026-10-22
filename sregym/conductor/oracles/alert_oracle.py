import json
import subprocess
import time

from sregym.conductor.oracles.alert_attribution import AlertScope, fault_scope
from sregym.conductor.oracles.base import Oracle

# Prometheus endpoint used from *inside* the prometheus-server pod via
# ``kubectl exec``.  We use localhost so the request doesn't depend on
# cluster DNS (which may be broken by fault-injection scenarios such as
# stale_coredns_config).
_PROMETHEUS_URL = "http://localhost:9090"

# How long to monitor for sustained alert silence.
_SUSTAINED_SILENCE_SECONDS = 120
_POLL_INTERVAL_SECONDS = 10
# Grace period before starting to check (let alerts resolve).
_BUFFER_SECONDS = 30


_INFRA_POD_PREFIXES = (
    "grafana",
    "prometheus",
    "alertmanager",
    "jaeger",
    "otel-collector",
    "opentelemetry",
    "opensearch",
    "blackbox",
    "observe",
    "promtail",
    "flagd",
    "load-generator",
    "loadgenerator",
)


class AlertOracle(Oracle):
    """Mitigation oracle that passes when no Prometheus alerts are firing.

    Queries the Prometheus alerts API for firing alerts scoped to the
    problem's namespace.  Because alerts can be flaky, the oracle waits
    for a sustained silence window before declaring success.

    Alerts originating from infrastructure components (grafana, prometheus,
    otel-collector, etc.) are excluded by default since they are not
    related to application faults.

    In a composite (``scope_to_fault``), only alerts attributable to this
    problem's own fault count (``alert_attribution.AlertScope``); other
    faults' and unrelated alerts are ignored.

    ``fetch_alerts`` returns the Prometheus ``/api/v1/alerts`` payload
    (default: ``kubectl exec`` into prometheus-server); ``clock`` and
    ``sleep`` drive the buffer and silence window.
    """

    importance = 1.0

    def __init__(
        self,
        problem,
        sustained_silence_seconds=_SUSTAINED_SILENCE_SECONDS,
        poll_interval_seconds=_POLL_INTERVAL_SECONDS,
        buffer_seconds=_BUFFER_SECONDS,
        exclude_alerts=None,
        include_infra_alerts=False,
        fetch_alerts=None,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        super().__init__(problem)
        self.sustained_silence_seconds = sustained_silence_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.buffer_seconds = buffer_seconds
        self.exclude_alerts = set(exclude_alerts or [])
        self.include_infra_alerts = include_infra_alerts
        self.fetch_alerts = fetch_alerts or self._fetch_prometheus_alerts
        self.clock = clock
        self.sleep = sleep
        # None: every alert in the namespace counts (a single fault).
        self.scope: AlertScope | None = None

    def scope_to_fault(self) -> None:
        self.scope = fault_scope(self.problem)

    # ------------------------------------------------------------------
    # Prometheus query helpers
    # ------------------------------------------------------------------

    def _fetch_prometheus_alerts(self) -> dict:
        """Return the Prometheus ``/api/v1/alerts`` payload.

        Uses ``kubectl exec`` into the prometheus-server pod with localhost
        so we don't depend on cluster DNS (which may be broken by fault
        injection, e.g. stale_coredns_config).
        """
        url = f"{_PROMETHEUS_URL}/api/v1/alerts"
        cmd = [
            "kubectl",
            "exec",
            "-n",
            "observe",
            "deploy/prometheus-server",
            "-c",
            "prometheus-server",
            "--",
            "wget",
            "-qO-",
            url,
        ]
        try:
            raw = subprocess.check_output(cmd, text=True, timeout=15)
            return json.loads(raw)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            raise RuntimeError("Failed to query Prometheus alerts") from exc

    def _query_firing_alerts(self, namespace: str) -> list[dict]:
        """Return the currently firing alerts in *namespace* that count for this oracle."""
        payload = self.fetch_alerts()
        firing = []
        for alert in payload.get("data", {}).get("alerts", []):
            if alert.get("state") != "firing":
                continue
            labels = alert.get("labels", {})
            if labels.get("namespace") != namespace:
                continue
            if labels.get("alertname") in self.exclude_alerts:
                continue
            if not self.include_infra_alerts:
                pod = labels.get("pod", "")
                svc = labels.get("service_name", "")
                source = pod or svc
                if any(source.startswith(p) for p in _INFRA_POD_PREFIXES):
                    continue
            if self.scope is not None and not self.scope.attributes(labels):
                continue
            firing.append(alert)
        return firing

    def _query_max_alert_for_duration(self) -> float:
        """Return the longest *for* duration (seconds) across all Prometheus alert rules.

        Queries ``/api/v1/rules`` via ``kubectl exec``.  Falls back to 0 if the
        query fails or no rules are found.
        """
        url = f"{_PROMETHEUS_URL}/api/v1/rules"
        cmd = [
            "kubectl",
            "exec",
            "-n",
            "observe",
            "deploy/prometheus-server",
            "-c",
            "prometheus-server",
            "--",
            "wget",
            "-qO-",
            url,
        ]
        try:
            raw = subprocess.check_output(cmd, text=True, timeout=15)
            payload = json.loads(raw)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            print(f"⚠️  Failed to query Prometheus rules: {exc}")
            return 0.0

        max_duration = 0.0
        for group in payload.get("data", {}).get("groups", []):
            for rule in group.get("rules", []):
                if rule.get("type") != "alerting":
                    continue
                duration = rule.get("duration", 0) or 0
                if duration > max_duration:
                    max_duration = duration
        return max_duration

    @staticmethod
    def _fmt_alert(alert: dict) -> str:
        labels = alert.get("labels", {})
        name = labels.get("alertname", "?")
        svc = labels.get("service_name") or labels.get("pod") or ""
        severity = labels.get("severity", "")
        return f"{name} ({svc}) [{severity}]"

    # ------------------------------------------------------------------
    # Oracle interface
    # ------------------------------------------------------------------

    def evaluate(self, solution=None, trace=None, duration=None) -> dict:
        print("== Alert Oracle Evaluation ==")

        namespace = self.problem.namespace

        # Buffer: give alerts time to resolve after mitigation.
        print(f"⏳ Waiting {self.buffer_seconds}s buffer before checking alerts…")
        self.sleep(self.buffer_seconds)

        # Poll for sustained silence. Any firing alert is an immediate failure.
        start = self.clock()
        last_log_second = -1

        while True:
            elapsed = self.clock() - start
            if elapsed >= self.sustained_silence_seconds:
                break

            firing = self._query_firing_alerts(namespace)

            if firing:
                names = ", ".join(self._fmt_alert(a) for a in firing)
                print(f"❌ Firing alerts in {namespace}{self._scope_note()}: {names}")
                return {"success": False}

            elapsed_int = int(elapsed)
            if elapsed_int >= last_log_second + 30:
                print(f"🔇 No alerts firing — silence for {elapsed_int}/{self.sustained_silence_seconds}s")
                last_log_second = elapsed_int

            self.sleep(self.poll_interval_seconds)

        print(f"✅ No alerts firing in {namespace}{self._scope_note()} for {self.sustained_silence_seconds}s")
        return {"success": True}

    def _scope_note(self) -> str:
        if self.scope is None:
            return ""
        return f" attributable to {type(self.problem).__name__} ({', '.join(sorted(self.scope.workloads))})"
