"""Which kind of cluster this process drives: a local kind cluster or a remote one (EKS, kubeadm).

Kind-only steps (``docker exec`` into node containers, ``kind load``, the kubeadm kube-proxy
image reset, Calico cleanup) must not run against a managed cluster. ``SREGYM_CLUSTER_PLATFORM``
(``kind``, ``eks`` or ``remote``) decides when set. Otherwise a cluster name from the parallel
runner's remote workers (``--worker-kubeconfig``, ``SREGYM_KIND_CLUSTER_NAME=remote-<hash>``)
means remote, and anything else keeps the historical default, kind. Nothing here talks to the
cluster, so the answer is stable and free.
"""

from __future__ import annotations

import os

PLATFORM_ENV = "SREGYM_CLUSTER_PLATFORM"
KIND = "kind"
REMOTE_PLATFORMS = frozenset({"eks", "remote"})
REMOTE_CLUSTER_PREFIX = "remote-"


def cluster_platform() -> str:
    """``kind``, ``eks`` or ``remote``."""
    explicit = os.environ.get(PLATFORM_ENV, "").strip().lower()
    if explicit:
        if explicit != KIND and explicit not in REMOTE_PLATFORMS:
            raise ValueError(f"{PLATFORM_ENV} must be one of kind, eks, remote; got {explicit!r}")
        return explicit
    if os.environ.get("SREGYM_KIND_CLUSTER_NAME", "").strip().startswith(REMOTE_CLUSTER_PREFIX):
        return "remote"
    return KIND


def is_kind_cluster() -> bool:
    return cluster_platform() == KIND
