# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Catalog family: Kubernetes-object faults (audit parts 2 and 3).

Pure module: standard library only; each entry's `build` imports its problem class lazily.

Conventions every entry follows:
- A draw's params are exactly the problem constructor's keyword arguments, so `build` is
  `ProblemClass(**draw.param_dict())`.
- Params are drawn from the same axes as the class's VariantSpec (`problems/variant_specs.py`),
  over every app the constructor accepts (`create_app`), not only the apps the registry lists.
- Footprints are field-level when the injector patches fields, and whole-object (field=None)
  when it deletes and re-applies or snapshot-restores the object. Delete+apply recovery
  re-applies a snapshot, which would revert any later fault on the same Deployment.
- Recovery snapshots are per fault (`VirtualizationFaultInjector._backup_path`,
  `/tmp/{ns}_{name}_{fault}_backup.yaml`), so a backup file is not a shared resource and
  needs no footprint; the whole-object footprint of a snapshot-restoring fault still keeps
  every other fault off that object.
- `kubectl rollout restart` writes `spec.template.metadata.annotations`; declaring that field
  keeps two restart-based faults off one Deployment (a stuck rollout would block the other).
- Tier RESTARTS_PODS for injectors that delete or recreate pods. Admission blockers
  (quotas, webhooks, PSA labels) live in `catalog/admission.py`.
"""

from __future__ import annotations

import importlib
import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint, Tier

HOTEL = "hotel_reservation"
SOCIAL = "social_network"
ASTRONOMY = "astronomy_shop"
TRAIN = "train_ticket"
ALL_APPS = frozenset({HOTEL, SOCIAL, ASTRONOMY, TRAIN})

# Copy of `_REPRESENTATIVE_SERVICES` in problems/variant_specs.py, which cannot be imported
# here (it imports every problem class). tests/composition/test_catalog_k8s.py fails on drift.
REPRESENTATIVE_SERVICES: Mapping[str, tuple[str, ...]] = {
    SOCIAL: (
        "user-service",
        "media-service",
        "compose-post-service",
        "post-storage-service",
        "social-graph-service",
        "url-shorten-service",
        "home-timeline-service",
        "text-service",
        "nginx-thrift",
    ),
    HOTEL: ("frontend", "geo", "profile", "rate", "recommendation", "search", "reservation", "user"),
    ASTRONOMY: (
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "frontend",
        "payment",
        "product-catalog",
        "recommendation",
        "shipping",
    ),
    TRAIN: (
        "ts-auth-service",
        "ts-order-service",
        "ts-route-service",
        "ts-seat-service",
        "ts-basic-service",
        "ts-travel-service",
        "ts-user-service",
        "ts-station-service",
    ),
}

_HOTEL_MONGODB = (
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
    "mongodb-user",
)

_PROBE_ERROR_MODES = ("wrong_path", "wrong_port", "wrong_protocol", "exec_false")
_REPLICA_COUNTS = (1, 3)

Footprints = tuple[Footprint, ...]
Params = dict[str, Any]


# ---------------------------------------------------------------- footprint vocabulary


def _deployment(name: str, field: str | None = None) -> Footprint:
    return Footprint("Deployment", name, field)


def _restart(service: str) -> Footprint:
    return _deployment(service, "spec.template.metadata.annotations")


def _delete_and_apply(service: str, params: Params) -> Footprints:
    """Injector rewrites the Deployment via delete+apply and recovery re-applies its snapshot."""
    return (_deployment(service),)


# ---------------------------------------------------------------- entry construction


@dataclass(frozen=True)
class ProblemBuilder:
    """`build` for every entry here: `<cls>(**draw.param_dict())` from `sregym.conductor.problems.<module>`."""

    module: str
    cls: str

    def __call__(self, draw: FaultDraw) -> Any:
        problem_class = getattr(importlib.import_module(f"sregym.conductor.problems.{self.module}"), self.cls)
        return problem_class(**draw.param_dict())


def _draw(key: str, app: str, params: Params, footprints: Footprints, tier: Tier, tags: frozenset[str]) -> FaultDraw:
    return FaultDraw(
        key=key,
        app=app,
        params=tuple(sorted(params.items())),
        footprints=footprints,
        tier=tier,
        exclusive_tags=tags,
    )


def _per_service(
    key: str,
    module: str,
    cls: str,
    *,
    footprints: Callable[[str, Params], Footprints],
    tier: Tier,
    axes: Mapping[str, Sequence[Any]] | None = None,
    services: Mapping[str, Sequence[str]] = REPRESENTATIVE_SERVICES,
    takes_app_name: bool = True,
    tags: frozenset[str] = frozenset(),
) -> CatalogEntry:
    """An entry whose draw picks one `faulty_service` of the app plus one value per axis."""
    axes = axes or {}

    def sample(app: str, rng: random.Random) -> FaultDraw:
        service = rng.choice(services[app])
        params: Params = {name: rng.choice(values) for name, values in axes.items()}
        params["faulty_service"] = service
        if takes_app_name:
            params["app_name"] = app
        return _draw(key, app, params, footprints(service, params), tier, tags)

    return CatalogEntry(key=key, apps=frozenset(services), sample=sample, build=ProblemBuilder(module, cls))


def _fixed(
    key: str,
    module: str,
    cls: str,
    *,
    app: str,
    footprints: Callable[[Params], Footprints],
    tier: Tier,
    axes: Mapping[str, Sequence[Any]] | None = None,
    tags: frozenset[str] = frozenset(),
) -> CatalogEntry:
    """An entry for a class hard-wired to one app; params are its constructor axes only."""
    axes = axes or {}

    def sample(drawn_app: str, rng: random.Random) -> FaultDraw:
        if drawn_app != app:
            raise ValueError(f"{key} supports only {app}, not {drawn_app}")
        params: Params = {name: rng.choice(values) for name, values in axes.items()}
        return _draw(key, app, params, footprints(params), tier, tags)

    return CatalogEntry(key=key, apps=frozenset({app}), sample=sample, build=ProblemBuilder(module, cls))


# ---------------------------------------------------------------- per-class footprints


def _wrong_service_selector(service: str, params: Params) -> Footprints:
    fps = (Footprint("Service", service, "spec.selector"),)
    if params["replica_count"] > 1:
        fps += (_deployment(service, "spec.replicas"),)
    return fps


def _wrong_dns_policy(service: str, params: Params) -> Footprints:
    return (
        _deployment(service, "spec.template.spec.dnsPolicy"),
        _deployment(service, "spec.template.spec.dnsConfig"),
        _restart(service),
    )


def _service_dns_netpol(service: str, params: Params) -> Footprints:
    # The policy copies the live Service selector at inject time, so a selector fault or a
    # deleted Service on the same svc would make it a silent no-op: declare the read.
    return (Footprint("NetworkPolicy", f"deny-dns-egress-{service}"), Footprint("Service", service, "spec.selector"))


# The injector deletes one fixed ConfigMap per app (IV:980-1006) and restarts the variant's
# derived service; it raises for any other app.
_MISSING_CONFIGMAP_SERVICE = {SOCIAL: ("media-mongodb",), HOTEL: ("mongodb-geo",)}
_MISSING_CONFIGMAP_OBJECT = {"media-mongodb": "media-mongodb", "mongodb-geo": "mongo-geo-script"}


def _missing_configmap(service: str, params: Params) -> Footprints:
    configmap = _MISSING_CONFIGMAP_OBJECT[service]
    return (Footprint("ConfigMap", configmap), _deployment(service, "spec.replicas"))


def _rolling_update(service: str, params: Params) -> Footprints:
    return (
        _deployment(service, "spec.strategy"),
        _deployment(service, "spec.template.spec.initContainers"),
        _restart(service),
    )


def _duplicate_pvc(service: str, params: Params) -> Footprints:
    return (*_delete_and_apply(service, params), Footprint("PersistentVolumeClaim", f"{service}-pvc"))


_PVC_SCOPE_SERVICES = {"single": _HOTEL_MONGODB[:1], "all": _HOTEL_MONGODB}


def _pvc_claim_mismatch(params: Params) -> Footprints:
    return tuple(
        fp for svc in _PVC_SCOPE_SERVICES[params["pvc_failure_scope"]] for fp in _delete_and_apply(svc, params)
    )


def _finalizer_deadlock(params: Params) -> Footprints:
    name = "cleanup-controller"
    return (
        Footprint("ClusterRole", "configmap-cleanup-controller"),
        Footprint("ClusterRoleBinding", "configmap-cleanup-controller"),
        Footprint("ServiceAccount", name),
        Footprint("ConfigMap", "cleanup-controller-script"),
        Footprint("ConfigMap", "reservation-cleanup-token"),
        _deployment(name),
    )


def _rbac_misconfiguration(params: Params) -> Footprints:
    service = params["faulty_service"]
    return (
        _deployment(service),
        Footprint("ConfigMap", "app-routing-config"),
        Footprint("ServiceAccount", f"{service}-rbac-sa"),
        Footprint("ClusterRole", f"{service}-rbac-role"),
        Footprint("ClusterRoleBinding", f"{service}-rbac-binding"),
    )


ENTRIES: tuple[CatalogEntry, ...] = (
    # --- Probes (delete+apply) ---
    _per_service(
        "readiness_probe_misconfiguration",
        "readiness_probe_misconfiguration",
        "ReadinessProbeMisconfiguration",
        axes={"probe_error_mode": _PROBE_ERROR_MODES, "replica_count": _REPLICA_COUNTS},
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "liveness_probe_misconfiguration",
        "liveness_probe_misconfiguration",
        "LivenessProbeMisconfiguration",
        axes={"probe_error_mode": _PROBE_ERROR_MODES, "replica_count": _REPLICA_COUNTS},
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "liveness_probe_too_aggressive",
        "liveness_probe_too_aggressive",
        "LivenessProbeTooAggressive",
        axes={"aggression_profile": ("aggressive", "slow_churn"), "startup_probe_guard": ("none", "short_startup")},
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    # --- Service selector / DNS ---
    _per_service(
        "wrong_service_selector",
        "wrong_service_selector",
        "WrongServiceSelector",
        axes={"replica_count": _REPLICA_COUNTS},
        footprints=_wrong_service_selector,
        tier=Tier.ORDINARY,
    ),
    _per_service(
        "wrong_dns_policy",
        "wrong_dns_policy",
        "WrongDNSPolicy",
        axes={"dns_misconfiguration_type": ("external_resolver", "default_policy")},
        footprints=_wrong_dns_policy,
        tier=Tier.RESTARTS_PODS,
    ),
    # Only the NetworkPolicy layer: the CoreDNS layer edits kube-system (EXCLUDE in the audit).
    _per_service(
        "service_dns_resolution_failure_netpol",
        "service_dns_resolution_failure",
        "ServiceDNSResolutionFailure",
        axes={"dns_failure_layer": ("network_policy_block_dns",), "dns_response_code": ("NXDOMAIN",)},
        footprints=_service_dns_netpol,
        tier=Tier.ORDINARY,
    ),
    # --- Missing resources ---
    # Deletes every pod in the namespace (IV:312), so it must precede faults that live in pods.
    _per_service(
        "missing_service",
        "missing_service",
        "MissingService",
        footprints=lambda svc, p: (Footprint("Service", svc),),
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "missing_configmap",
        "missing_configmap",
        "MissingConfigMap",
        axes={"configmap_fault_mode": ("delete",)},
        services=_MISSING_CONFIGMAP_SERVICE,
        footprints=_missing_configmap,
        tier=Tier.RESTARTS_PODS,
    ),
    # --- Storage ---
    _per_service(
        "duplicate_pvc_mounts",
        "duplicate_pvc_mounts",
        "DuplicatePVCMounts",
        footprints=_duplicate_pvc,
        tier=Tier.RESTARTS_PODS,
    ),
    _fixed(
        "pvc_claim_mismatch",
        "pvc_claim_mismatch",
        "PVCClaimMismatch",
        app=HOTEL,
        axes={"pvc_failure_scope": ("single", "all")},
        footprints=_pvc_claim_mismatch,
        tier=Tier.RESTARTS_PODS,
    ),
    # --- Ports ---
    _per_service(
        "k8s_target_port_misconfig",
        "target_port",
        "K8STargetPortMisconfig",
        axes={"bad_port": (9999, 8888, 7777, 6666)},
        footprints=lambda svc, p: (Footprint("Service", svc, "spec.ports.targetPort"),),
        tier=Tier.ORDINARY,
    ),
    _per_service(
        "sidecar_port_conflict",
        "sidecar_port_conflict",
        "SidecarPortConflict",
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    # Two instances would compete for the node's hostPort 9100 with each other.
    _per_service(
        "service_port_conflict",
        "service_port_conflict",
        "ServicePortConflict",
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
        tags=frozenset({"hostPort:9100"}),
    ),
    # --- Rollout / init / resources ---
    # Snapshot-restores the whole Deployment on recovery (its own backup file).
    _per_service(
        "init_container_dependency_hang",
        "init_container_dependency_hang",
        "InitContainerDependencyHang",
        axes={"init_hang_mechanism": ("dns_lookup_loop", "tcp_connect_loop", "http_ready_check")},
        footprints=lambda svc, p: (_deployment(svc),),
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "rolling_update_misconfigured",
        "rolling_update_misconfigured",
        "RollingUpdateMisconfigured",
        axes={
            "init_hang_mode": ("sleep_infinity", "crash_loop", "slow_timeout"),
            "strategy_profile": ("drain_all", "stuck_canary"),
        },
        footprints=_rolling_update,
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "resource_request_too_large",
        "resource_request",
        "ResourceRequestTooLarge",
        axes={"resource_type": ("memory", "cpu")},
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    _per_service(
        "resource_request_too_small",
        "resource_request",
        "ResourceRequestTooSmall",
        axes={"resource_type": ("memory",)},
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    # --- Scheduling ---
    # Only hostname topology: the zone variant is a no-op on clusters without zone labels.
    _per_service(
        "pod_anti_affinity_deadlock",
        "pod_anti_affinity_deadlock",
        "PodAntiAffinityDeadlock",
        axes={"anti_affinity_topology": ("hostname",)},
        services={SOCIAL: REPRESENTATIVE_SERVICES[SOCIAL]},
        takes_app_name=False,
        footprints=_delete_and_apply,
        tier=Tier.RESTARTS_PODS,
    ),
    _fixed(
        "assign_to_non_existent_node",
        "assign_non_existent_node",
        "AssignNonExistentNode",
        app=SOCIAL,
        footprints=lambda p: _delete_and_apply("user-service", p),
        tier=Tier.RESTARTS_PODS,
    ),
    _fixed(
        "scale_pod_zero_social_net",
        "scale_pod",
        "ScalePodSocialNet",
        app=SOCIAL,
        footprints=lambda p: (_deployment("user-service", "spec.replicas"),),
        tier=Tier.RESTARTS_PODS,
    ),
    # --- Hotel-only workload faults ---
    # Health-gated on frontend (metrics, rollout, HPA condition): owns the whole frontend Deployment.
    _fixed(
        "hpa_missing_effective_cpu_request",
        "hpa_missing_effective_cpu_request",
        "HPAMissingEffectiveCPURequest",
        app=HOTEL,
        footprints=lambda p: (_deployment("frontend"), Footprint("HorizontalPodAutoscaler", "frontend-capacity")),
        tier=Tier.RESTARTS_PODS,
    ),
    _fixed(
        "service_wrong_pod_selection_hotel_reservation",
        "service_wrong_pod_selection_hotel_reservation",
        "ServiceWrongPodSelectionHotelReservation",
        app=HOTEL,
        footprints=lambda p: (
            Footprint("Service", "frontend", "spec.selector"),
            _deployment("frontend", "spec.template.metadata.labels"),
            _deployment("search", "spec.template.metadata.labels"),
        ),
        tier=Tier.RESTARTS_PODS,
    ),
    _fixed(
        "finalizer_deadlock_controller",
        "finalizer_deadlock_controller",
        "FinalizerDeadlockController",
        app=HOTEL,
        axes={"rbac_break_mode": ("remove_write_verbs", "drop_subresource_rule", "add_resourcenames_restriction")},
        footprints=_finalizer_deadlock,
        tier=Tier.ORDINARY,
    ),
    _fixed(
        "rbac_misconfiguration",
        "rbac_misconfiguration",
        "RBACMisconfiguration",
        app=ASTRONOMY,
        axes={"faulty_service": ("frontend",)},
        footprints=_rbac_misconfiguration,
        tier=Tier.RESTARTS_PODS,
    ),
)
