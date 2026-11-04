"""Process driver: freeze and thaw a workload's main process with SIGSTOP/SIGCONT.

A container's init process ignores signals sent from inside its own pid namespace, so the freeze
comes from the node (host pid, ``kill -STOP``). The pod stays Running and Ready (the app declares
no probes) and its kernel still accepts connections, but nothing answers. The thaw works from
inside the pod too (``kill -CONT 1`` is delivered because the stopped process has default
disposition for CONT), which is what a cluster operator with only kubectl would use.
"""

from __future__ import annotations

from sregym.generators.fault.components.kube import ComponentError, exec_in, pod_for
from sregym.generators.fault.components.node import ContainerRef, Nodes


def state(nodes: Nodes, ref: ContainerRef) -> str:
    """Process state letter from ``/proc/<pid>/status`` on the node (``S`` sleeping, ``T`` stopped)."""
    out = nodes.run(ref, ["sh", "-c", f"grep '^State:' /proc/{ref.pid}/status"])
    parts = out.split()
    if len(parts) < 2:
        raise ComponentError(f"no process state for pid {ref.pid} on {ref.node}: {out[:100]!r}")
    return parts[1]


def current(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None) -> ContainerRef:
    return nodes.locate(namespace, pod_for(nodes.kube, namespace, kind, workload), container)


def freeze(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None) -> ContainerRef:
    """SIGSTOP the container's init process from the node; raises unless it is then stopped."""
    ref = current(nodes, namespace, kind, workload, container)
    nodes.run(ref, ["kill", "-STOP", str(ref.pid)])
    if (s := state(nodes, ref)) != "T":
        raise ComponentError(f"{workload} in {namespace}: pid {ref.pid} on {ref.node} is {s!r} after SIGSTOP")
    return ref


def is_frozen(nodes: Nodes, namespace: str, kind: str, workload: str, container: str | None) -> tuple[bool, str]:
    """Is the workload's current main process stopped? A replaced pod is a new process: not frozen."""
    try:
        ref = current(nodes, namespace, kind, workload, container)
        s = state(nodes, ref)
    except ComponentError as e:
        return True, f"{workload} not running a live process yet: {str(e)[:150]}"
    return s == "T", f"{workload} pod {ref.pod} pid {ref.pid} state {s}"


def thaw(kube, namespace: str, kind: str, workload: str, container: str | None) -> None:
    """SIGCONT through the pod (``kill -CONT 1``): the repair an operator with kubectl has."""
    exec_in(
        kube, namespace, pod_for(kube, namespace, kind, workload), ["sh", "-c", "kill -CONT 1"], container=container
    )
