"""Load drivers: hold connections open against one service, or flood an entry point with requests.

Both run from a pod outside the app namespace (``foreign``). The hog uses the TARGET's own image
(it ships bash, whose ``/dev/tcp`` opens a connection per file descriptor in one cheap process), so
no extra image is needed. Each connection costs the server a thread; thousands exhaust its memory
or thread budget while the pod stays Running. The hog re-opens its connections when the server
drops them (a restart), so restarting the victim does not end the fault.
"""

from __future__ import annotations

from sregym.conductor.problems.composition.grading import Request, wget_command
from sregym.generators.fault.components import foreign
from sregym.generators.fault.components.kube import Kube, pod_for

HOG = "connection-hog"
SURGE = "traffic-surge"


def hog_script(host: str, port: int, connections: int) -> str:
    """Open ``connections`` TCP connections, hold them, and start over when the server drops them."""
    return (
        "while :; do ( "
        f"for i in $(seq {connections}); do exec {{fd}}<>/dev/tcp/{host}/{port} || break; done; "
        "sleep 5; "
        # held connections are alive while a read times out (rc>128); EOF or reset (rc 1) means the server
        # dropped them: leave the subshell (closing every connection) and start over
        "while read -t 1 -u 10 _ 2>/dev/null; rc=$?; [ $rc -gt 128 ]; do sleep 5; done "
        "); sleep 2; done"
    )


def start_hog(
    kube: Kube, namespace: str, kind: str, workload: str, container: str | None, host: str, port: int, connections: int
) -> str:
    image = foreign.image_of(kube, namespace, pod_for(kube, namespace, kind, workload), container)
    manifest = foreign.pod_manifest(
        HOG, namespace, workload, image, hog_script(host, port, connections), shell="bash", memory_limit="512Mi"
    )
    return foreign.start(kube, manifest)


def surge_script(base: str, requests: tuple[Request, ...], workers: int) -> str:
    """``workers`` background loops, each cycling through the requests with no pause."""
    body = "; ".join(wget_command(base, r) + " >/dev/null 2>&1" for r in requests)
    return f"for w in $(seq {workers}); do (while :; do {body}; done) & done; wait"


def start_surge(
    kube: Kube, namespace: str, workload: str, base: str, requests: tuple[Request, ...], workers: int, cpu_limit: str
) -> str:
    manifest = foreign.pod_manifest(
        SURGE, namespace, workload, "busybox:1.36", surge_script(base, requests, workers), cpu_limit=cpu_limit
    )
    return foreign.start(kube, manifest)
