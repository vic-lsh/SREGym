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
"""Catalog family: faults that block or rewrite pod admission for the whole namespace.

Audit part 5 (§1 hazard 5). Every entry here is BLOCKS_ADMISSION, so it is injected after every
injector that needs a new pod to come up. None of these injectors needs a pod admitted in the app
namespace, so they compose with each other in the tail, except for the fixed-name and ordering
exclusions below, expressed as exclusive tags:

- AdmissionWebhookOutage and AdmissionWebhookTLSMismatch create the same webhook name.
- AdmissionWebhookTLSMismatch and CumulativeAdmissionWebhookTimeout share namespace
  `policy-system`, and TLSMismatch recovery deletes it.
- MutatingWebhookResourceLimits must run first in the tail: its pre-patch rollout of nginx-thrift
  (mutating_webhook_resource_limits.py:584-650, raising) needs pod admission. A Tier cannot order
  within the tail, so it excludes the other social_network blockers instead.

AdmissionWebhookOutage, AdmissionWebhookTLSMismatch and PSARestrictedBlocksRecreation are
registered for hotel_reservation only but support all four apps; the catalog uses all four.
"""

from __future__ import annotations

import random
from typing import Any

from sregym.conductor.problems.composition.catalog._kit import (
    ALL_APPS,
    APP_NAMESPACES,
    REPRESENTATIVE_SERVICES,
    Pick,
    deployment,
    entry,
    fixed,
)
from sregym.conductor.problems.composition.spec import CatalogEntry, Footprint, Tier

_WEBHOOK_KINDS = {
    "validating": "ValidatingWebhookConfiguration",
    "mutating": "MutatingWebhookConfiguration",
}
_POD_POLICY_WEBHOOK = "pod-policy.validation.k8s.io"
_POLICY_NAMESPACE = "policy-system"

_TAG_POD_POLICY_WEBHOOK = f"webhook:{_POD_POLICY_WEBHOOK}"
_TAG_POLICY_NAMESPACE = f"namespace:{_POLICY_NAMESPACE}"

_OUTAGE = "admission_webhook_outage"
_TLS_MISMATCH = "admission_webhook_tls_mismatch"
_PSA = "psa_restricted_blocks_recreation"
_MUTATING = "mutating_webhook_resource_limits_social_network"
_CUMULATIVE = "cumulative_admission_webhook_timeout_hotel_reservation"
_NAMESPACE_MEMORY = "namespace_memory_limit"


def _after_mutating(key: str) -> str:
    """Tag shared by MutatingWebhookResourceLimits and one blocker it must precede."""
    return f"order:{_MUTATING}-before-{key}"


# Blockers that can share a social_network composite with MutatingWebhookResourceLimits.
_MUTATING_MUST_PRECEDE = (_OUTAGE, _TLS_MISMATCH, _PSA)


def _webhook_choose(key: str, extra: tuple[Footprint, ...], tags: frozenset[str]):
    order_tags = frozenset({_after_mutating(key)}) if key in _MUTATING_MUST_PRECEDE else frozenset()

    def choose(app: str, rng: random.Random) -> Pick:
        svc = rng.choice(REPRESENTATIVE_SERVICES[app])
        kind = rng.choice(sorted(_WEBHOOK_KINDS))
        return Pick(
            {"faulty_service": svc, "webhook_kind": kind},
            (Footprint(_WEBHOOK_KINDS[kind], _POD_POLICY_WEBHOOK), *extra),
            Tier.BLOCKS_ADMISSION,
            tags | order_tags,
        )

    return choose


def _choose_psa(app: str, rng: random.Random) -> Pick:
    svc = rng.choice(REPRESENTATIVE_SERVICES[app])
    return Pick(
        {"faulty_service": svc},
        (Footprint("Namespace", APP_NAMESPACES[app], "metadata.labels"),),
        Tier.BLOCKS_ADMISSION,
        frozenset({_after_mutating(_PSA)}),
    )


def _build_outage(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.admission_webhook_outage import AdmissionWebhookOutage

    return AdmissionWebhookOutage(app_name=app, **params)


def _build_tls_mismatch(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.admission_webhook_tls_mismatch import AdmissionWebhookTLSMismatch

    return AdmissionWebhookTLSMismatch(app_name=app, **params)


def _build_psa(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.psa_restricted_blocks_recreation import PSARestrictedBlocksRecreation

    return PSARestrictedBlocksRecreation(app_name=app, **params)


def _build_mutating(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.mutating_webhook_resource_limits import MutatingWebhookResourceLimits

    return MutatingWebhookResourceLimits()


def _build_cumulative(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.cumulative_admission_webhook_timeout_hotel_reservation import (
        CumulativeAdmissionWebhookTimeoutHotelReservation,
    )

    return CumulativeAdmissionWebhookTimeoutHotelReservation()


def _build_namespace_memory(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.namespace_memory_limit import NamespaceMemoryLimit

    return NamespaceMemoryLimit()


_DECOY_MUTATING_WEBHOOKS = (
    "cert-manager-webhook",
    "istio-sidecar-injector",
    "kyverno-resource-mutating-webhook-cfg",
    "linkerd-proxy-injector-webhook-config",
)
_CUMULATIVE_WEBHOOKS = (
    "pod-resource-validator",
    "audit-log-enforcer",
    "image-policy-checker",
    "tenant-quota-validator",
)


def _mwc(name: str) -> Footprint:
    return Footprint("MutatingWebhookConfiguration", name)


ENTRIES: tuple[CatalogEntry, ...] = (
    entry(
        _OUTAGE,
        ALL_APPS,
        _webhook_choose(_OUTAGE, (), frozenset({_TAG_POD_POLICY_WEBHOOK})),
        _build_outage,
    ),
    entry(
        _TLS_MISMATCH,
        ALL_APPS,
        _webhook_choose(
            _TLS_MISMATCH,
            (Footprint("Namespace", _POLICY_NAMESPACE),),
            frozenset({_TAG_POD_POLICY_WEBHOOK, _TAG_POLICY_NAMESPACE}),
        ),
        _build_tls_mismatch,
    ),
    entry(_PSA, ALL_APPS, _choose_psa, _build_psa),
    # Whole nginx-thrift: it patches the memory fields and its pre-patch rollout needs the pod
    # healthy when its turn comes, so any earlier fault on nginx-thrift breaks its injection.
    entry(
        _MUTATING,
        ("social_network",),
        fixed(
            Pick(
                {},
                (
                    deployment("nginx-thrift"),
                    _mwc("gatekeeper-mutating-webhook-configuration"),
                    *(_mwc(name) for name in _DECOY_MUTATING_WEBHOOKS),
                    Footprint("Namespace", "platform-ops"),
                ),
                Tier.BLOCKS_ADMISSION,
                frozenset(_after_mutating(key) for key in _MUTATING_MUST_PRECEDE),
            )
        ),
        _build_mutating,
    ),
    entry(
        _CUMULATIVE,
        ("hotel_reservation",),
        fixed(
            Pick(
                {},
                (
                    *(_mwc(name) for name in _CUMULATIVE_WEBHOOKS + _DECOY_MUTATING_WEBHOOKS),
                    Footprint("Namespace", _POLICY_NAMESPACE),
                ),
                Tier.BLOCKS_ADMISSION,
                frozenset({_TAG_POLICY_NAMESPACE}),
            )
        ),
        _build_cumulative,
    ),
    # The quota rejects every later pod without memory limits; the injector then deletes
    # `search`'s current ReplicaSet (KubeCtl.get_matching_replicasets returns the current one
    # first, so the audit's P5 stale-ReplicaSet no-op is fixed) and recovery forces
    # replicas=1. `search` therefore has no admitted pods while this fault is live, which would
    # mask any other fault on it: own the whole Deployment. Recovery deletes every memory
    # quota, hence the tag.
    entry(
        _NAMESPACE_MEMORY,
        ("hotel_reservation",),
        fixed(
            Pick(
                {},
                (Footprint("ResourceQuota", "memory-limit-quota"), deployment("search")),
                Tier.BLOCKS_ADMISSION,
                frozenset({"ResourceQuota:memory"}),
            )
        ),
        _build_namespace_memory,
    ),
)
