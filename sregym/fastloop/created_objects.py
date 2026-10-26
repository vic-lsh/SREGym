"""Cluster-scoped objects an injection created, so a warm lane's recovery removes exactly those.

A fault may install infrastructure outside the app namespace (a Helm release in its own
namespace, its ClusterRoles, IngressClass and admission webhooks). Its own ``recover_fault``
usually reverts only the fault, and a warm lane has no namespace teardown or cluster reconcile
after it, so those objects would survive into the next row. The worker lists the cluster-scoped
inventory right before and right after ``inject_fault``; the difference is what the injection
created. After full recovery it deletes exactly that difference and waits until it is gone.
Objects that existed before the injection are never touched, and no name is guessed.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

# Deletion order: admission and API registrations first, so nothing still routes requests to a
# backend being removed; RBAC before the namespaces whose service accounts it binds; namespaces
# last, since deleting one removes everything inside it.
CLUSTER_KINDS: tuple[str, ...] = (
    "ValidatingWebhookConfiguration",
    "MutatingWebhookConfiguration",
    "APIService",
    "IngressClass",
    "CustomResourceDefinition",
    "ClusterRoleBinding",
    "ClusterRole",
    "PriorityClass",
    "StorageClass",
    "Namespace",
)


@dataclass(frozen=True, order=True)
class ClusterObject:
    kind: str
    name: str

    def __post_init__(self) -> None:
        if self.kind not in CLUSTER_KINDS:
            raise ValueError(f"unsupported cluster-scoped kind {self.kind!r}")
        if not isinstance(self.name, str) or not self.name:
            raise ValueError(f"{self.kind} name must be a nonempty string")

    def __str__(self) -> str:
        return f"{self.kind}/{self.name}"


class ClusterInventory(Protocol):
    def list_objects(self) -> frozenset[ClusterObject]:
        """Every object of ``CLUSTER_KINDS``; raises when any kind cannot be listed."""
        ...

    def delete(self, obj: ClusterObject) -> None:
        """Request deletion; an already absent object is not an error."""
        ...


def created_between(before: Iterable[ClusterObject], after: Iterable[ClusterObject]) -> frozenset[ClusterObject]:
    return frozenset(after) - frozenset(before)


def _deletion_order(obj: ClusterObject) -> tuple[int, str]:
    return CLUSTER_KINDS.index(obj.kind), obj.name


def remove_created(
    inventory: ClusterInventory,
    created: Iterable[ClusterObject],
    *,
    timeout_s: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    poll_s: float = 2.0,
) -> tuple[frozenset[ClusterObject], str]:
    """Delete ``created`` and wait until none is listed; returns ``(still present, last error)``.

    Namespace deletion is asynchronous and a terminating namespace still counts as present, so
    the wait ends only when the API server no longer lists the object.
    """
    remaining = frozenset(created)
    error = ""
    deadline = clock() + timeout_s
    requested = False
    while remaining:
        if not requested:
            error = ""
            for obj in sorted(remaining, key=_deletion_order):
                try:
                    inventory.delete(obj)
                except Exception as exc:  # retried below until the deadline
                    error = f"delete {obj}: {type(exc).__name__}: {str(exc)[:200]}"
            requested = not error
        try:
            remaining = remaining & inventory.list_objects()
        except Exception as exc:
            error = f"list cluster objects: {type(exc).__name__}: {str(exc)[:200]}"
        if not remaining or clock() >= deadline:
            break
        sleep(poll_s)
    return remaining, error


class KubeClusterInventory:
    """``ClusterInventory`` over the harness's (un-proxied) Kubernetes clients."""

    def __init__(self, apis: dict[str, Any] | None = None):
        """``apis`` maps API names to clients; by default the real clients, built on first use
        (after the problem factory has loaded the harness kubeconfig)."""
        self._given = apis

    def _api(self, name: str) -> Any:
        if self._given is None:
            from kubernetes import client

            self._given = {
                "core": client.CoreV1Api(),
                "rbac": client.RbacAuthorizationV1Api(),
                "admission": client.AdmissionregistrationV1Api(),
                "apiregistration": client.ApiregistrationV1Api(),
                "apiextensions": client.ApiextensionsV1Api(),
                "networking": client.NetworkingV1Api(),
                "scheduling": client.SchedulingV1Api(),
                "storage": client.StorageV1Api(),
            }
        return self._given[name]

    # kind -> (API, list method, delete method)
    _METHODS = {
        "ValidatingWebhookConfiguration": (
            "admission",
            "list_validating_webhook_configuration",
            "delete_validating_webhook_configuration",
        ),
        "MutatingWebhookConfiguration": (
            "admission",
            "list_mutating_webhook_configuration",
            "delete_mutating_webhook_configuration",
        ),
        "APIService": ("apiregistration", "list_api_service", "delete_api_service"),
        "IngressClass": ("networking", "list_ingress_class", "delete_ingress_class"),
        "CustomResourceDefinition": (
            "apiextensions",
            "list_custom_resource_definition",
            "delete_custom_resource_definition",
        ),
        "ClusterRoleBinding": ("rbac", "list_cluster_role_binding", "delete_cluster_role_binding"),
        "ClusterRole": ("rbac", "list_cluster_role", "delete_cluster_role"),
        "PriorityClass": ("scheduling", "list_priority_class", "delete_priority_class"),
        "StorageClass": ("storage", "list_storage_class", "delete_storage_class"),
        "Namespace": ("core", "list_namespace", "delete_namespace"),
    }

    def list_objects(self) -> frozenset[ClusterObject]:
        objects = set()
        for kind in CLUSTER_KINDS:
            api, method, _ = self._METHODS[kind]
            for item in getattr(self._api(api), method)().items:
                objects.add(ClusterObject(kind, item.metadata.name))
        return frozenset(objects)

    def delete(self, obj: ClusterObject) -> None:
        from kubernetes.client.rest import ApiException

        api, _, method = self._METHODS[obj.kind]
        try:
            getattr(self._api(api), method)(name=obj.name)
        except ApiException as exc:
            if exc.status != 404:
                raise
