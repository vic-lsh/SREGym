"""Objects an injection created, so a warm lane's recovery removes exactly those.

A fault may install infrastructure outside the app namespace (a Helm release in its own
namespace, its ClusterRoles, IngressClass and admission webhooks) or add objects inside it (an
HPA the baseline lacked). Its own ``recover_fault`` usually reverts only the fault, and a warm
lane has no namespace teardown or cluster reconcile after it, so those objects would survive
into the next row. The worker lists the inventory (cluster-scoped kinds plus the app namespace's
stable kinds) right before and right after ``inject_fault``; the difference is what the injection
created. After full recovery it deletes exactly that difference and waits until it is gone; an
injection-created object that cannot be removed fails recovery. Objects that existed before the
injection are never touched, and no name is guessed. Any other difference from the pre-injection
inventory (a responder's repair, say) is an ``InventoryDrift`` eval note, never a deletion or a
block. Only kinds and names are recorded, never values.
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
# App-namespace kinds whose names are stable across rollouts (no Pods, ReplicaSets, Endpoints,
# Events or Leases, which churn by design). Secrets are left out so the harness never reads one.
NAMESPACED_KINDS: tuple[str, ...] = (
    "HorizontalPodAutoscaler",
    "PodDisruptionBudget",
    "CronJob",
    "Ingress",
    "NetworkPolicy",
    "Deployment",
    "StatefulSet",
    "DaemonSet",
    "Service",
    "ConfigMap",
    "PersistentVolumeClaim",
    "RoleBinding",
    "Role",
    "ServiceAccount",
    "ResourceQuota",
    "LimitRange",
)
# Webhooks and API registrations go before anything they could intercept; app-namespace objects
# go before the cluster RBAC and namespaces they may depend on.
DELETION_ORDER: tuple[str, ...] = CLUSTER_KINDS[:3] + NAMESPACED_KINDS + CLUSTER_KINDS[3:]


@dataclass(frozen=True, order=True)
class ClusterObject:
    """One object by kind and name; ``namespace`` is empty exactly for cluster-scoped kinds."""

    kind: str
    name: str
    namespace: str = ""

    def __post_init__(self) -> None:
        if self.kind in CLUSTER_KINDS:
            if self.namespace:
                raise ValueError(f"{self.kind} is cluster-scoped; got namespace {self.namespace!r}")
        elif self.kind in NAMESPACED_KINDS:
            if not isinstance(self.namespace, str) or not self.namespace:
                raise ValueError(f"{self.kind} is namespaced; namespace must be a nonempty string")
        else:
            raise ValueError(f"unsupported kind {self.kind!r}")
        if not isinstance(self.name, str) or not self.name:
            raise ValueError(f"{self.kind} name must be a nonempty string")

    def __str__(self) -> str:
        return f"{self.kind}/{self.namespace}/{self.name}" if self.namespace else f"{self.kind}/{self.name}"


class ClusterInventory(Protocol):
    def list_objects(self, namespaces: frozenset[str]) -> frozenset[ClusterObject]:
        """Every object of ``CLUSTER_KINDS``, plus every object of ``NAMESPACED_KINDS`` in
        ``namespaces``; raises when any kind cannot be listed."""
        ...

    def delete(self, obj: ClusterObject) -> None:
        """Request deletion; an already absent object is not an error."""
        ...


def created_between(before: Iterable[ClusterObject], after: Iterable[ClusterObject]) -> frozenset[ClusterObject]:
    return frozenset(after) - frozenset(before)


def in_namespaces(objects: Iterable[ClusterObject], namespaces: frozenset[str]) -> frozenset[ClusterObject]:
    return frozenset(obj for obj in objects if obj.namespace in namespaces)


@dataclass(frozen=True)
class InventoryDrift:
    """How the app namespaces differ from their pre-injection baseline, by kind and name only.

    An eval note, not a failure: objects other than the injection's own may be a responder's
    legitimate repair, so the worker labels the run and the next row instead of deleting them.
    """

    extra: tuple[ClusterObject, ...] = ()
    missing: tuple[ClusterObject, ...] = ()

    @classmethod
    def between(cls, baseline: Iterable[ClusterObject], current: Iterable[ClusterObject]) -> InventoryDrift:
        baseline, current = frozenset(baseline), frozenset(current)
        return cls(tuple(sorted(current - baseline)), tuple(sorted(baseline - current)))

    def __bool__(self) -> bool:
        return bool(self.extra or self.missing)

    def __str__(self) -> str:
        parts = []
        if self.extra:
            parts.append("not in the pre-injection baseline: " + ", ".join(map(str, self.extra)))
        if self.missing:
            parts.append("missing from the pre-injection baseline: " + ", ".join(map(str, self.missing)))
        return "; ".join(parts)

    def to_note(self) -> dict[str, Any]:
        return {
            "label": "eval",
            "kind": "app_namespace_inventory_drift",
            "not_in_baseline": [str(obj) for obj in self.extra],
            "missing_from_baseline": [str(obj) for obj in self.missing],
        }


def inventory_drift(baseline: Iterable[ClusterObject], current: Iterable[ClusterObject]) -> str:
    """Values-free description of how ``current`` differs from ``baseline``; empty when equal."""
    return str(InventoryDrift.between(baseline, current))


def _deletion_order(obj: ClusterObject) -> tuple[int, str, str]:
    return DELETION_ORDER.index(obj.kind), obj.namespace, obj.name


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
            remaining = remaining & inventory.list_objects(
                frozenset(obj.namespace for obj in remaining if obj.namespace)
            )
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
                "apps": client.AppsV1Api(),
                "autoscaling": client.AutoscalingV2Api(),
                "batch": client.BatchV1Api(),
                "policy": client.PolicyV1Api(),
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
    # kind -> (API, resource suffix of list_namespaced_* and delete_namespaced_*)
    _NAMESPACED = {
        "HorizontalPodAutoscaler": ("autoscaling", "horizontal_pod_autoscaler"),
        "PodDisruptionBudget": ("policy", "pod_disruption_budget"),
        "CronJob": ("batch", "cron_job"),
        "Ingress": ("networking", "ingress"),
        "NetworkPolicy": ("networking", "network_policy"),
        "Deployment": ("apps", "deployment"),
        "StatefulSet": ("apps", "stateful_set"),
        "DaemonSet": ("apps", "daemon_set"),
        "Service": ("core", "service"),
        "ConfigMap": ("core", "config_map"),
        "PersistentVolumeClaim": ("core", "persistent_volume_claim"),
        "RoleBinding": ("rbac", "role_binding"),
        "Role": ("rbac", "role"),
        "ServiceAccount": ("core", "service_account"),
        "ResourceQuota": ("core", "resource_quota"),
        "LimitRange": ("core", "limit_range"),
    }

    def list_objects(self, namespaces: frozenset[str]) -> frozenset[ClusterObject]:
        objects = set()
        for kind in CLUSTER_KINDS:
            api, method, _ = self._METHODS[kind]
            for item in getattr(self._api(api), method)().items:
                objects.add(ClusterObject(kind, item.metadata.name))
        for namespace in sorted(namespaces):
            for kind in NAMESPACED_KINDS:
                api, resource = self._NAMESPACED[kind]
                for item in getattr(self._api(api), f"list_namespaced_{resource}")(namespace).items:
                    objects.add(ClusterObject(kind, item.metadata.name, namespace))
        return frozenset(objects)

    def delete(self, obj: ClusterObject) -> None:
        from kubernetes.client.rest import ApiException

        try:
            if obj.namespace:
                api, resource = self._NAMESPACED[obj.kind]
                getattr(self._api(api), f"delete_namespaced_{resource}")(name=obj.name, namespace=obj.namespace)
                return
            api, _, method = self._METHODS[obj.kind]
            getattr(self._api(api), method)(name=obj.name)
        except ApiException as exc:
            if exc.status != 404:
                raise
