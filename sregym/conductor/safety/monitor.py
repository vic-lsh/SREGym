"""Latching background monitor that samples safety invariants across the trajectory.

Lifecycle :
  * ``arm()`` is called SYNCHRONOUSLY at the post-fault / pre-agent baseline moment
    (end of ``Conductor.start_problem``) to eliminate the arm/agent-start race.
  * ``run()`` loops until stopped, sampling each not-yet-latched invariant.
  * ``sample_now()`` lets the conductor take a fresh sample right before grading.
  * ``stop()`` takes one final sample, then joins — called at the top of
    ``Conductor._finish_problem`` before fault recovery mutates the cluster.
Once an invariant returns a violation it is LATCHED and never re-checked/cleared.
"""

from __future__ import annotations

import contextlib
import logging
import threading

from sregym.conductor.safety.invariants import InvariantContext, SafetyInvariant, Violation


class InvariantMonitor(threading.Thread):
    def __init__(
        self,
        problem,
        kubectl,
        invariants: list[SafetyInvariant],
        namespace: str,
        problem_id: str | None = None,
        poll_interval: float = 5.0,
        logger: logging.Logger | None = None,
        enable_watch: bool = True,
    ):
        super().__init__(daemon=True, name="safety-invariant-monitor")
        self.ctx = InvariantContext(problem, kubectl, namespace, problem_id)
        self.invariants = list(invariants)
        self.poll_interval = poll_interval
        self.logger = logger or logging.getLogger("all.sregym.safety")
        # Edge-triggered tier. Polling alone cannot see a cheat that is undone
        # inside the sample gap; the watch layer judges event payloads, which
        # still carry the forbidden value after a revert. Polling stays armed
        # underneath as a backstop, so disabling this only loses latency.
        self.enable_watch = enable_watch
        self._watcher = None
        self._watch_kinds: dict[str, list] = {}
        # Routing keyed by (kind, namespace) — NOT kind alone. An invariant must
        # only receive events from the streams it actually asked for; see
        # _on_watch_event for the cross-namespace false positive this prevents.
        self._watch_routes: dict[tuple, list] = {}
        self._stop = threading.Event()
        self._lock = threading.Lock()
        # Serializes _sample_once() so inv.check(ctx) for a given invariant is
        # never re-entered concurrently by the run() loop and the conductor
        # thread (sample_now()/stop()). Separate from _lock, which only guards
        # the _violations dict; must NOT be held while waiting on poll_interval.
        self._sample_lock = threading.Lock()
        self._violations: dict[str, Violation] = {}
        self._armed = False

    # --- lifecycle ---------------------------------------------------------
    def arm(self) -> None:
        """Capture the baseline synchronously. Safe to call once."""
        if self._armed:
            return
        for inv in self.invariants:
            try:
                inv.arm(self.ctx)
            except Exception as e:  # arming failure disables that invariant for the run
                self.logger.warning(f"[safety] arm({inv.name}) failed (invariant disabled for run): {e}")
        self._armed = True
        self.logger.info(f"[safety] armed {len(self.invariants)} invariant(s): {[i.name for i in self.invariants]}")
        if self.enable_watch:
            self._start_watch()

    def _start_watch(self) -> None:
        """Subscribe to the streams the armed invariants declared. Best-effort:
        any failure leaves the polling backstop as the sole tier."""
        try:
            from sregym.conductor.safety.watch import WatchMultiplexer

            specs = []
            for inv in self.invariants:
                try:
                    inv_specs = inv.watch_specs(self.ctx) or []
                except Exception as e:
                    self.logger.warning(f"[safety] watch_specs({inv.name}) failed (poll-only): {e}")
                    continue
                for spec in inv_specs:
                    self._watch_kinds.setdefault(spec.kind, [])
                    if inv not in self._watch_kinds[spec.kind]:
                        self._watch_kinds[spec.kind].append(inv)
                    route = self._watch_routes.setdefault((spec.kind, spec.namespace), [])
                    if inv not in route:
                        route.append(inv)
                    specs.append(spec)
            if not specs:
                return
            api_client = getattr(getattr(self.ctx.kubectl, "core_v1_api", None), "api_client", None)
            if api_client is None:
                self.logger.warning("[safety] no api_client available; watch tier disabled (poll-only)")
                return
            self._watcher = WatchMultiplexer(api_client, self._on_watch_event)
            self._watcher.start(specs)
        except Exception as e:
            self.logger.warning(f"[safety] watch tier failed to start (poll-only): {e}")
            self._watcher = None

    def _on_watch_event(self, kind: str, event_type: str, obj: dict) -> None:
        """Route one watch event to the invariants that asked for this kind.

        An invariant that implements ``evaluate_object`` judges the payload, so a
        forbidden value that has already been reverted is still caught. One that
        does not falls back to an immediate ``check()`` — better latency, but it
        re-reads live state and so cannot see a reverted value.

        Routing is by (kind, NAMESPACE). Keying on kind alone fanned every stream
        out to every invariant watching that kind, across namespace boundaries:
        UntrustedImageDeployed watches kube-system daemonsets for
        WorkloadImbalance, so PodSecurityBaselineDowngrade — which only ever asked
        for its own app namespace — received a kube-system event, found no
        baseline entry for it (its baseline is keyed by name, not namespace),
        treated it as agent-created, and latched on a privileged DaemonSet that
        had existed in the cluster for 20 days.
        """
        event_ns = (obj.get("metadata") or {}).get("namespace")
        # Invariants that asked for this kind in this namespace, plus those that
        # asked for it cluster-wide (namespace None, e.g. cluster-scoped kinds).
        recipients = list(self._watch_routes.get((kind, event_ns), ()))
        for inv in self._watch_routes.get((kind, None), ()):
            if inv not in recipients:
                recipients.append(inv)
        for inv in recipients:
            with self._lock:
                if inv.name in self._violations:
                    continue
            try:
                v = inv.evaluate_object(self.ctx, kind, event_type, obj)
            except Exception as e:
                self.logger.warning(f"[safety] evaluate_object({inv.name}) raised (fail-open): {e}")
                continue
            if v is NotImplemented:
                try:
                    v = inv.check(self.ctx)
                except Exception as e:
                    self.logger.warning(f"[safety] check({inv.name}) raised on event (fail-open): {e}")
                    continue
            if v is not None:
                self._latch(inv, v, source="watch")

    def _latch(self, inv, v: Violation, source: str = "poll") -> None:
        """Record a violation once. First writer wins; never cleared."""
        with self._lock:
            if inv.name in self._violations:
                return
            self._violations[inv.name] = v
        self.logger.warning(f"[safety] VIOLATION latched ({source}): {inv.name} — {v.detail}")

    def run(self) -> None:
        if not self._armed:
            self.arm()
        while not self._stop.is_set():
            self._sample_once()
            self._stop.wait(self.poll_interval)

    def sample_now(self) -> None:
        """Take one immediate sample (used by the conductor just before grading)."""
        self._sample_once()

    def stop(self, timeout: float = 10.0) -> None:
        # one final sample to catch a last-moment change before teardown
        with contextlib.suppress(Exception):
            self._sample_once()
        self._stop.set()
        if self._watcher is not None:
            with contextlib.suppress(Exception):
                self._watcher.stop()
        try:
            if self.is_alive():
                self.join(timeout=timeout)
        except Exception:
            pass

    # --- sampling ----------------------------------------------------------
    def _sample_once(self) -> None:
        # Hold the sampling lock around the whole body so check() for a given
        # invariant is never re-entered concurrently (run() loop vs. the
        # conductor thread's sample_now()/stop()). Never held while waiting on
        # poll_interval, so the run loop's cadence is unaffected.
        with self._sample_lock:
            for inv in self.invariants:
                with self._lock:
                    if inv.name in self._violations:
                        continue
                try:
                    v = inv.check(self.ctx)
                except Exception as e:  # fail open on any probe error
                    self.logger.warning(f"[safety] check({inv.name}) raised (fail-open, ignored): {e}")
                    v = None
                if v is not None:
                    self._latch(inv, v, source="poll")

    # --- reads -------------------------------------------------------------
    def snapshot_violations(self, stage: str | None = None) -> list[dict]:
        with self._lock:
            out = []
            for v in self._violations.values():
                d = v.to_dict()
                if stage and not d.get("stage"):
                    d["stage"] = stage
                out.append(d)
            return out

    def has_violation(self) -> bool:
        with self._lock:
            return len(self._violations) > 0

    def coverage(self) -> dict:
        """What was actually being observed, so a caller can tell 'observed and
        clean' apart from 'never observed'.

        An empty violation list on its own does not mean the trajectory was
        clean: every check fails open, so an unreadable object and a compliant
        one look identical. This reports the watch tier's health alongside it.
        """
        watcher = self._watcher
        return {
            "watch_enabled": bool(self.enable_watch),
            "watch_active": watcher is not None,
            "watch_events_seen": watcher.events_seen() if watcher else 0,
            "watch_degraded": watcher.degraded() if watcher else {},
            "watched_kinds": sorted(self._watch_kinds),
            "poll_interval": self.poll_interval,
            "invariants": [i.name for i in self.invariants],
        }
