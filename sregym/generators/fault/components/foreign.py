"""Pods the injector runs OUTSIDE the app namespace: a load generator, a connection hog, a CPU burner.

They stand for another tenant or client of the cluster, not for part of the app, so they live in an
injector namespace and never in the app's. Their absence is what repairs the fault (an operator
finds the noisy client and stops it). Everything is labelled with the app namespace and fault
kind so a fault finds only its own pods.
"""

from __future__ import annotations

import contextlib
import json
import time
from typing import Any

from sregym.generators.fault.components.kube import CLIENT_NAMESPACE, ComponentError, Kube

FOREIGN_NAMESPACE = CLIENT_NAMESPACE
LABEL = "sregym.io/injected-fault"


def _name(kind: str, namespace: str, workload: str) -> str:
    return f"{kind}-{namespace}-{workload}".replace("_", "-")[:60].rstrip("-")


def pod_manifest(
    kind: str,
    namespace: str,
    workload: str,
    image: str,
    script: str,
    *,
    cpu_request: str = "50m",
    cpu_limit: str = "500m",
    memory_limit: str = "256Mi",
    node: str | None = None,
    restart: str = "Always",
    shell: str = "sh",
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "restartPolicy": restart,
        "automountServiceAccountToken": False,
        "terminationGracePeriodSeconds": 0,
        "containers": [
            {
                "name": "injector",
                "image": image,
                "imagePullPolicy": "IfNotPresent",
                "command": [shell, "-c", script],
                "resources": {
                    "requests": {"cpu": cpu_request, "memory": "32Mi"},
                    "limits": {"cpu": cpu_limit, "memory": memory_limit},
                },
            }
        ],
    }
    if node:
        spec["nodeName"] = node
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": _name(kind, namespace, workload),
            "namespace": FOREIGN_NAMESPACE,
            "labels": {LABEL: kind, "sregym.io/app-namespace": namespace, "sregym.io/target": workload},
        },
        "spec": spec,
    }


def start(kube: Kube, manifest: dict[str, Any], wait_running_s: float = 120.0) -> str:
    """Create the injector namespace if needed and the pod; wait until Running. Returns the pod name."""
    ns = manifest["metadata"]["namespace"]
    kube.run(
        ["apply", "-f", "-"], json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}}).encode()
    )
    kube.run(["apply", "-f", "-"], json.dumps(manifest).encode())
    name = manifest["metadata"]["name"]
    deadline = time.monotonic() + wait_running_s
    while time.monotonic() < deadline:
        phase = kube.run(["get", "pod", name, "-n", ns, "-o", "jsonpath={.status.phase}"]).decode()
        if phase == "Running":
            return name
        time.sleep(1)
    raise ComponentError(f"injector pod {name} in {ns} not Running after {wait_running_s}s")


def exists(kube: Kube, kind: str, namespace: str, workload: str) -> bool:
    out = kube.run(
        ["get", "pod", _name(kind, namespace, workload), "-n", FOREIGN_NAMESPACE, "--ignore-not-found", "-o", "name"]
    )
    return bool(out.strip())


def stop(kube: Kube, kind: str, namespace: str, workload: str) -> None:
    """Delete the injector pod (the reference repair, and the cleanup); no-op if absent."""
    with contextlib.suppress(ComponentError):
        kube.run(
            [
                "delete",
                "pod",
                _name(kind, namespace, workload),
                "-n",
                FOREIGN_NAMESPACE,
                "--ignore-not-found",
                "--grace-period=0",
                "--force",
                "--wait=true",
            ]
        )


def image_of(kube: Kube, namespace: str, pod: str, container: str | None) -> str:
    obj = kube.json(["get", "pod", pod, "-n", namespace])
    containers = obj["spec"]["containers"]
    c = next((c for c in containers if container in (None, c["name"])), None)
    if c is None:
        raise ComponentError(f"pod {pod} in {namespace} has no container {container}")
    return c["image"]
