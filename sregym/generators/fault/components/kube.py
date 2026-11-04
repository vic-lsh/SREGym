"""The one Kubernetes seam of the component drivers: kubectl argv in, bytes out.

Every driver takes a ``Kube``; tests pass a fake that records argv and returns canned output.
Also holds the app-neutral workload actions every component fault reuses as references or
decoys: rollout-restart, delete the workload's pods, re-apply accepted source, and run a short
in-namespace client pod.
"""

from __future__ import annotations

import contextlib
import json
import secrets
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from sregym.conductor.problems.composition import grading


class ComponentError(RuntimeError):
    """A driver step failed; the message names the namespace, workload and command."""


@dataclass
class Kube:
    """kubectl through a subprocess; ``kubectl`` may carry ``--kubeconfig``/``--context`` flags."""

    kubectl: tuple[str, ...] = ("kubectl",)
    timeout_s: float = 120.0

    def run(self, argv: list[str], stdin: bytes | None = None, *, timeout_s: float | None = None) -> bytes:
        cmd = [*self.kubectl, *argv]
        try:
            out = subprocess.run(
                cmd, input=stdin, capture_output=True, timeout=timeout_s or self.timeout_s, check=False
            )
        except subprocess.TimeoutExpired as e:
            raise ComponentError(f"timed out: {' '.join(cmd)[:300]}") from e
        if out.returncode != 0:
            err = out.stderr.decode(errors="replace").strip()
            raise ComponentError(f"exit {out.returncode}: {' '.join(cmd)[:300]}: {err[:500]}")
        return out.stdout

    def json(self, argv: list[str]) -> dict:
        return json.loads(self.run([*argv, "-o", "json"]))


def ready_pods(kube: Kube, namespace: str, kind: str, name: str) -> list[str]:
    """Names of the workload's Running, non-terminating pods, oldest first."""
    obj = kube.json(["get", kind.lower(), name, "-n", namespace])
    labels = (obj.get("spec", {}).get("selector") or {}).get("matchLabels") or {}
    if not labels:
        raise ComponentError(f"{kind}/{name} in {namespace} has no matchLabels selector")
    selector = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    pods = kube.json(["get", "pods", "-n", namespace, "-l", selector]).get("items") or []
    live = [
        p
        for p in pods
        if (p.get("status") or {}).get("phase") == "Running" and not (p.get("metadata") or {}).get("deletionTimestamp")
    ]
    live.sort(key=lambda p: p["metadata"].get("creationTimestamp", ""))
    return [p["metadata"]["name"] for p in live]


def pod_for(kube: Kube, namespace: str, kind: str, name: str) -> str:
    pods = ready_pods(kube, namespace, kind, name)
    if not pods:
        raise ComponentError(f"no Running pod for {kind}/{name} in {namespace}")
    return pods[0]


def exec_in(
    kube: Kube, namespace: str, pod: str, argv: list[str], *, container: str | None = None, stdin: bytes | None = None
) -> str:
    flags = ["-i"] if stdin is not None else []
    if container:
        flags += ["-c", container]
    return kube.run(["exec", *flags, "-n", namespace, pod, "--", *argv], stdin).decode(errors="replace")


def rollout_restart(kube: Kube, namespace: str, kind: str, name: str, timeout_s: int = 300) -> None:
    """``kubectl rollout restart`` and wait for it: the generic "restart the workload" action."""
    ref = f"{kind.lower()}/{name}"
    kube.run(["rollout", "restart", ref, "-n", namespace])
    kube.run(["rollout", "status", ref, "-n", namespace, f"--timeout={timeout_s}s"], timeout_s=timeout_s + 30)


def delete_pods(kube: Kube, namespace: str, kind: str, name: str, timeout_s: int = 300) -> None:
    """Delete the workload's pods and wait for replacements: "restart the stateful pod"."""
    for pod in ready_pods(kube, namespace, kind, name):
        kube.run(["delete", "pod", pod, "-n", namespace, "--wait=true"], timeout_s=timeout_s)
    kube.run(
        ["rollout", "status", f"{kind.lower()}/{name}", "-n", namespace, f"--timeout={timeout_s}s"],
        timeout_s=timeout_s + 30,
    )


def restore_source(kube: Kube, namespace: str, render: Path | None) -> None:
    """Re-apply the accepted source render (``SREGYM_ACCEPTED_RENDER``) to the namespace: the generic
    "restore everything from source" repair. For a fault that changed no Kubernetes object it is a
    no-op, which is what makes it a decoy. Raises if no render is known (never guesses one)."""
    if render is None or not render.is_file():
        raise ComponentError(f"restore_source: no accepted render file ({render})")
    kube.run(["apply", "-n", namespace, "-f", str(render)], timeout_s=300)
    kube.run(["rollout", "status", "deployment", "-n", namespace, "--timeout=300s"], timeout_s=330)


def run_client_pod(
    kube: Kube, namespace: str, script: str, deadline_s: int = 120, prefix: str = "sregym-client"
) -> str:
    """Run ``script`` in a short-lived busybox probe pod in ``namespace`` and return its log.

    The pod carries the probe label, so the composite's collateral walk ignores it; it is deleted
    on every exit path. Raises if it does not finish in ``deadline_s``.
    """
    name = f"{prefix}-{secrets.token_hex(4)}"
    manifest = grading.client_pod_manifest(name, namespace, script, deadline_s)
    kube.run(["apply", "-f", "-"], json.dumps(manifest).encode())
    try:
        phase = "Pending"
        deadline = time.monotonic() + deadline_s + 30
        while time.monotonic() < deadline:
            phase = kube.run(["get", "pod", name, "-n", namespace, "-o", "jsonpath={.status.phase}"]).decode()
            if phase in ("Succeeded", "Failed"):
                break
            time.sleep(1)
        log = kube.run(["logs", name, "-n", namespace]).decode(errors="replace")
        if phase not in ("Succeeded", "Failed"):
            raise ComponentError(f"client pod {name} in {namespace} did not finish (phase {phase})")
        return log
    finally:
        with contextlib.suppress(ComponentError):
            kube.run(["delete", "pod", name, "-n", namespace, "--grace-period=0", "--wait=false"])
