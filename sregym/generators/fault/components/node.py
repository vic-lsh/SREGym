"""The node seam: run a command on the Kubernetes node that hosts a pod, and find a container's host pid.

Node-level faults (freeze a process, shape a pod's network) need a root shell on the node. On a
kind cluster the node is a docker container named after the Kubernetes node, so the shell is
``docker exec <node> ...``. ``NodeShell`` is the one seam: tests pass a fake, other clusters
would pass their own (ssh, a privileged debug pod). Everything here is app-neutral: it works from
a pod name and a container name, found through the pod spec.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from typing import Protocol

from sregym.generators.fault.components.kube import ComponentError, Kube


class NodeShell(Protocol):
    def run(self, node: str, argv: list[str], *, timeout_s: float = 60.0) -> str: ...


@dataclass
class DockerNode:
    """kind: the node ``<name>`` is the docker container ``<name>``."""

    docker: tuple[str, ...] = ("docker",)

    def run(self, node: str, argv: list[str], *, timeout_s: float = 60.0) -> str:
        cmd = [*self.docker, "exec", node, *argv]
        try:
            out = subprocess.run(cmd, capture_output=True, timeout=timeout_s, check=False)
        except subprocess.TimeoutExpired as e:
            raise ComponentError(f"timed out: {' '.join(cmd)[:300]}") from e
        if out.returncode != 0:
            raise ComponentError(
                f"exit {out.returncode}: {' '.join(cmd)[:300]}: {out.stderr.decode(errors='replace')[:400]}"
            )
        return out.stdout.decode(errors="replace")


@dataclass(frozen=True)
class ContainerRef:
    """One running container: where it is and its init process as the node sees it."""

    namespace: str
    pod: str
    container: str
    node: str
    container_id: str
    pid: int


@dataclass
class Nodes:
    """Node access for drivers: the shell plus the Kube used to read pod specs."""

    kube: Kube
    shell: NodeShell = field(default_factory=DockerNode)

    def locate(self, namespace: str, pod: str, container: str | None = None) -> ContainerRef:
        """The container's node, id and host pid (``crictl inspect``), from the pod spec and status."""
        obj = self.kube.json(["get", "pod", pod, "-n", namespace])
        node = (obj.get("spec") or {}).get("nodeName")
        if not node:
            raise ComponentError(f"pod {pod} in {namespace} is not scheduled on a node")
        name = container or obj["spec"]["containers"][0]["name"]
        status = next(
            (s for s in (obj.get("status") or {}).get("containerStatuses") or [] if s.get("name") == name), None
        )
        cid = ((status or {}).get("containerID") or "").split("://", 1)[-1]
        if not cid or "running" not in (status or {}).get("state", {}):
            raise ComponentError(f"container {name} of pod {pod} in {namespace} is not running")
        out = self.shell.run(node, ["crictl", "inspect", "-o", "go-template", "--template", "{{.info.pid}}", cid])
        try:
            pid = int(out.strip())
        except ValueError as e:
            raise ComponentError(f"crictl inspect {cid[:12]} on {node} returned {out[:100]!r}") from e
        if pid <= 0:
            raise ComponentError(f"container {name} of pod {pod} has no host pid on {node}")
        return ContainerRef(namespace, pod, name, node, cid, pid)

    def run(self, ref: ContainerRef, argv: list[str], *, timeout_s: float = 60.0) -> str:
        return self.shell.run(ref.node, argv, timeout_s=timeout_s)
