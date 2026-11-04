"""Noisy-neighbor driver: a CPU-burning pod in another namespace, pinned to the app pod's node.

It competes for the node's CPU with every pod there. Its CPU request sets its cgroup weight, so a
large request starves co-located pods that only asked for a little. The pod stays outside the app
namespace; stopping it is the repair.
"""

from __future__ import annotations

from sregym.generators.fault.components import foreign
from sregym.generators.fault.components.kube import ComponentError, Kube, pod_for

NEIGHBOR = "noisy-neighbor"


def node_of(kube: Kube, namespace: str, kind: str, workload: str) -> str:
    pod = pod_for(kube, namespace, kind, workload)
    node = kube.json(["get", "pod", pod, "-n", namespace]).get("spec", {}).get("nodeName")
    if not node:
        raise ComponentError(f"pod {pod} in {namespace} is not scheduled")
    return node


def burn_script(workers: int) -> str:
    return f"for w in $(seq {workers}); do (while :; do :; done) & done; wait"


def start(
    kube: Kube, namespace: str, kind: str, workload: str, *, workers: int, cpu_request: str, cpu_limit: str
) -> str:
    node = node_of(kube, namespace, kind, workload)
    manifest = foreign.pod_manifest(
        NEIGHBOR,
        namespace,
        workload,
        "busybox:1.36",
        burn_script(workers),
        cpu_request=cpu_request,
        cpu_limit=cpu_limit,
        node=node,
    )
    return foreign.start(kube, manifest)
