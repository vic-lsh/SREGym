"""Edge-triggered watch layer for safety invariants (watch tier).

The polling monitor can only see a cheat that is still in place when a sample
lands. Measured against a real cluster at the default 5s cadence, a Service
flipped to NodePort and reverted after 1s was detected 0/3 times, after 3s 1/3,
and only from ~6s onward 3/3. Anything an agent does and undoes inside the gap is
invisible, and latching cannot help because nothing was ever sampled.

This module closes that gap by subscribing to the Kubernetes watch API. The API
server pushes ADDED/MODIFIED/DELETED with the object AS IT WAS at that revision,
so a do-then-undo delivers two events and the forbidden intermediate value is
carried in the first one — even if the object is already reverted by the time we
react.

Two ways an invariant can use it (see ``SafetyInvariant`` in invariants.py):

  * ``evaluate_object(ctx, kind, event_type, obj)`` — preferred. The invariant
    judges the EVENT PAYLOAD, so a reverted value is still caught. This is the
    only form that actually closes the gap.
  * no override — the monitor falls back to running the invariant's ordinary
    ``check(ctx)`` immediately on any relevant event. That improves latency (react
    on change instead of on a timer) but re-reads live state, so a value already
    reverted is missed. Honest fallback, not a fix.

Design constraints carried over from the polling tier: strictly read-only,
fail-open on every error, and never block the caller. A watch that dies is
recorded as a DEGRADED stream rather than silently reported as "clean" — the
polling loop remains armed as a backstop underneath.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("all.sregym.safety")

# Default per-stream server-side timeout. The API server closes the stream at
# this point and we simply re-establish; it bounds how long a silently-wedged
# connection can go unnoticed.
_STREAM_TIMEOUT_SECONDS = 300


class WatchSpec:
    """One resource stream an invariant wants to observe.

    ``kind`` is the lowercase plural used elsewhere in the safety package
    ("services", "deployments", ...) so it lines up with ``probes.get_items``.
    ``namespace`` of None means cluster-scoped.
    """

    __slots__ = ("kind", "namespace")

    def __init__(self, kind: str, namespace: str | None = None):
        self.kind = kind
        self.namespace = namespace

    def key(self) -> tuple:
        return (self.kind, self.namespace)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"WatchSpec({self.kind}, ns={self.namespace})"


# kind -> (client class name, list-method name). Declared as plain data so the
# set of watchable kinds can be asserted in tests WITHOUT constructing API
# clients: an invariant that declares a kind missing from these tables gets a
# stream that never starts and silently degrades to poll-only, which is a bug
# that should fail a test rather than only show up in a live coverage report.
NAMESPACED_KINDS = {
    "services": ("CoreV1Api", "list_namespaced_service"),
    "configmaps": ("CoreV1Api", "list_namespaced_config_map"),
    "pods": ("CoreV1Api", "list_namespaced_pod"),
    "persistentvolumeclaims": ("CoreV1Api", "list_namespaced_persistent_volume_claim"),
    "deployments": ("AppsV1Api", "list_namespaced_deployment"),
    "statefulsets": ("AppsV1Api", "list_namespaced_stateful_set"),
    "daemonsets": ("AppsV1Api", "list_namespaced_daemon_set"),
    "ingresses": ("NetworkingV1Api", "list_namespaced_ingress"),
    "networkpolicies": ("NetworkingV1Api", "list_namespaced_network_policy"),
}
CLUSTER_KINDS = {
    "namespaces": ("CoreV1Api", "list_namespace"),
    "persistentvolumes": ("CoreV1Api", "list_persistent_volume"),
    "validatingwebhookconfigurations": ("AdmissionregistrationV1Api", "list_validating_webhook_configuration"),
    "mutatingwebhookconfigurations": ("AdmissionregistrationV1Api", "list_mutating_webhook_configuration"),
}


def resolves(kind: str, namespaced: bool) -> bool:
    """True if a WatchSpec for this kind/scope can actually start a stream."""
    return kind in (NAMESPACED_KINDS if namespaced else {**CLUSTER_KINDS, **NAMESPACED_KINDS})


def _list_fn_for(api_client, kind: str, namespace: str | None) -> Callable | None:
    """Resolve ``kind`` to a client list method suitable for ``Watch.stream``.

    Returns None for a kind we do not know how to watch, which the caller treats
    as "no watch coverage for this kind" and leaves to the polling backstop.
    """
    from kubernetes import client as k8s

    entry = NAMESPACED_KINDS.get(kind) if namespace else (CLUSTER_KINDS.get(kind) or NAMESPACED_KINDS.get(kind))
    if entry is None:
        return None
    api_name, method = entry
    return getattr(getattr(k8s, api_name)(api_client), method)


class _Stream(threading.Thread):
    """A single resilient watch stream, re-establishing across expiry/410."""

    def __init__(
        self,
        spec: WatchSpec,
        api_client,
        on_event: Callable[[str, str, dict], None],
        stop_event: threading.Event,
        on_degraded: Callable[[WatchSpec, str], None],
    ):
        super().__init__(daemon=True, name=f"safety-watch-{spec.kind}")
        self.spec = spec
        self._api_client = api_client
        self._on_event = on_event
        self._stop = stop_event
        self._on_degraded = on_degraded
        self.events_seen = 0

    def run(self) -> None:  # pragma: no cover - exercised live, not in unit tests
        from kubernetes import watch as k8s_watch

        list_fn = _list_fn_for(self._api_client, self.spec.kind, self.spec.namespace)
        if list_fn is None:
            self._on_degraded(self.spec, "no watch support for kind")
            return

        kwargs: dict[str, Any] = {"timeout_seconds": _STREAM_TIMEOUT_SECONDS}
        if self.spec.namespace:
            kwargs["namespace"] = self.spec.namespace

        resource_version: str | None = None
        while not self._stop.is_set():
            w = k8s_watch.Watch()
            try:
                stream_kwargs = dict(kwargs)
                if resource_version:
                    stream_kwargs["resource_version"] = resource_version
                for event in w.stream(list_fn, **stream_kwargs):
                    if self._stop.is_set():
                        break
                    etype = event.get("type", "")
                    obj = event.get("object")
                    raw = self._to_dict(obj)
                    if raw is None:
                        continue
                    rv = (raw.get("metadata") or {}).get("resourceVersion")
                    if rv:
                        resource_version = rv
                    self.events_seen += 1
                    try:
                        self._on_event(self.spec.kind, etype, raw)
                    except Exception as e:
                        logger.warning(f"[safety] watch handler for {self.spec.kind} raised (ignored): {e}")
            except Exception as e:
                # 410 Gone means our resourceVersion is too old: drop it and
                # re-list from scratch. Anything else: back off and retry, but
                # record that this stream was not continuously observing.
                if _is_expired(e):
                    resource_version = None
                else:
                    self._on_degraded(self.spec, f"{type(e).__name__}: {e}")
                    self._stop.wait(2.0)
            finally:
                with contextlib.suppress(Exception):
                    w.stop()

    @staticmethod
    def _to_dict(obj) -> dict | None:
        """Convert a typed client model to the camelCase dict shape the
        invariants already parse (same shape as ``kubectl get -o json``)."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj
        try:
            from kubernetes import client as k8s

            return k8s.ApiClient().sanitize_for_serialization(obj)
        except Exception:
            return None


def _is_expired(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    if status == 410:
        return True
    return "too old resource version" in str(exc).lower()


class WatchMultiplexer:
    """Owns one stream per distinct (kind, namespace) and fans events out.

    Deduplicates specs across invariants so N invariants watching Services cost
    one stream. Never raises to the caller: a stream that cannot start is
    recorded as degraded and the polling backstop covers that kind.
    """

    def __init__(self, api_client, on_event: Callable[[str, str, dict], None]):
        self._api_client = api_client
        self._on_event = on_event
        self._stop = threading.Event()
        self._streams: dict[tuple, _Stream] = {}
        self._degraded: dict[tuple, str] = {}
        self._lock = threading.Lock()

    def start(self, specs: list[WatchSpec]) -> None:
        for spec in specs:
            key = spec.key()
            if key in self._streams:
                continue
            try:
                s = _Stream(spec, self._api_client, self._on_event, self._stop, self._mark_degraded)
                s.start()
                self._streams[key] = s
            except Exception as e:
                self._mark_degraded(spec, f"start failed: {type(e).__name__}: {e}")
        live = len(self._streams) - len(self._degraded)
        logger.info(f"[safety] watch layer: {live} live stream(s), {len(self._degraded)} degraded")

    def _mark_degraded(self, spec: WatchSpec, reason: str) -> None:
        with self._lock:
            first = spec.key() not in self._degraded
            self._degraded[spec.key()] = reason
        if first:
            logger.warning(f"[safety] watch stream DEGRADED for {spec.kind} (ns={spec.namespace}): {reason}")

    def degraded(self) -> dict:
        """Kinds we are NOT continuously observing, so a caller can distinguish
        'observed and clean' from 'never observed'."""
        with self._lock:
            return {f"{k[0]}@{k[1] or '*'}": v for k, v in self._degraded.items()}

    def events_seen(self) -> int:
        return sum(s.events_seen for s in self._streams.values())

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        for s in self._streams.values():
            try:
                if s.is_alive():
                    s.join(timeout=timeout / max(1, len(self._streams)))
            except Exception:
                pass
