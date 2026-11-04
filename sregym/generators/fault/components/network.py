"""Network driver: netem delay or loss on a pod's egress, applied in the pod's netns from the node.

``nsenter -t <host pid> -n tc qdisc ...`` shapes the pod's own interface without touching any
Kubernetes object. The qdisc lives in the pod sandbox: it survives container restarts and dies
with the pod, so replacing the pod clears it. Only the target's egress is shaped (its replies and
its calls to others).
"""

from __future__ import annotations

import re

from sregym.generators.fault.components.kube import ComponentError, pod_for
from sregym.generators.fault.components.node import ContainerRef, Nodes

MODES = ("delay", "loss")


def netem_args(mode: str, severity: int) -> list[str]:
    """``delay`` severity is milliseconds, ``loss`` severity is percent."""
    if mode not in MODES:
        raise ValueError(f"unknown network mode {mode!r}; one of {MODES}")
    if severity <= 0 or (mode == "loss" and severity > 100):
        raise ValueError(f"bad {mode} severity {severity}")
    return ["delay", f"{severity}ms"] if mode == "delay" else ["loss", f"{severity}%"]


def _netns(ref: ContainerRef, tail: list[str]) -> list[str]:
    return ["nsenter", "-t", str(ref.pid), "-n", *tail]


def interface(nodes: Nodes, ref: ContainerRef) -> str:
    """The pod's default-route device."""
    out = nodes.run(ref, _netns(ref, ["ip", "-o", "route", "show", "default"]))
    m = re.search(r"\bdev (\S+)", out)
    if not m:
        raise ComponentError(f"no default route in the netns of {ref.pod} ({ref.namespace}): {out[:100]!r}")
    return m.group(1)


def impair(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None, mode: str, severity: int):
    """Install the qdisc; raises unless it is then listed. Returns the ContainerRef."""
    args = netem_args(mode, severity)
    ref = nodes.locate(namespace, pod_for(nodes.kube, namespace, kind, workload), container)
    dev = interface(nodes, ref)
    nodes.run(ref, _netns(ref, ["tc", "qdisc", "replace", "dev", dev, "root", "netem", *args]))
    if "netem" not in nodes.run(ref, _netns(ref, ["tc", "qdisc", "show", "dev", dev])):
        raise ComponentError(f"{workload} in {namespace}: netem not listed on {dev} after install")
    return ref


def is_impaired(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None) -> tuple[bool, str]:
    """Does the workload's current pod still have a netem qdisc? A replaced pod does not."""
    try:
        ref = nodes.locate(namespace, pod_for(nodes.kube, namespace, kind, workload), container)
        out = nodes.run(ref, _netns(ref, ["tc", "qdisc", "show", "dev", interface(nodes, ref)]))
    except ComponentError as e:
        return True, f"{workload} not ready to inspect: {str(e)[:150]}"
    return "netem" in out, f"{workload} pod {ref.pod}: {' '.join(out.split())[:120]}"


def clear(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None) -> None:
    """Remove the qdisc (the reference repair). No-op if there is none."""
    ref = nodes.locate(namespace, pod_for(nodes.kube, namespace, kind, workload), container)
    dev = interface(nodes, ref)
    if "netem" in nodes.run(ref, _netns(ref, ["tc", "qdisc", "show", "dev", dev])):
        nodes.run(ref, _netns(ref, ["tc", "qdisc", "del", "dev", dev, "root"]))
