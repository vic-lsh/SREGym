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
"""Which composite faults need their own recovery, and which the teardown already removes?

Pure module: standard library only. After a problem the conductor deletes the app namespace
(`Application.cleanup`) and then reconciles cluster state to its baseline
(`service/cluster_state.py` `reconcile_to_baseline`). A fault whose every write lands in the app
namespace is therefore gone without running its `recover_fault`, which for N=8 saves up to
8 x 240 s.

The reconcile is incomplete. It only deletes cluster objects *added* since the baseline, never
PriorityClasses, never edits to pre-existing cluster objects (nodes, webhooks, the CoreDNS
ConfigMap aside), never the `ingress-nginx` namespace, and never ClusterRoles or namespaces whose
names contain `-w` or `-run`. So the rule fails closed: a fault is skipped only when every
footprint kind and every exclusive tag is positively known to live in the app namespace.
Anything cluster-scoped, host-level (khaos), undeclared (legacy entries) or unclassified is
recovered. `tests/composition/test_recovery.py` checks that no real catalog draw is
unclassified, so a new kind or tag scope must be added here before it ships.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from dataclasses import dataclass

from sregym.conductor.problems.composition.spec import FaultDraw

# Kinds whose objects live in the app namespace and are deleted with it.
NAMESPACED_KINDS: frozenset[str] = frozenset(
    {
        "ConfigMap",
        "CronJob",
        "DaemonSet",
        "Deployment",
        "Endpoints",
        "HorizontalPodAutoscaler",
        "Ingress",
        "Job",
        "LimitRange",
        "NetworkPolicy",
        "PersistentVolumeClaim",
        "Pod",
        "PodDisruptionBudget",
        "ReplicaSet",
        "ResourceQuota",
        "Role",
        "RoleBinding",
        "Secret",
        "Service",
        "ServiceAccount",
        "StatefulSet",
    }
)
# Cluster-scoped kinds. `Namespace` is cluster-scoped unless it is the app namespace itself.
CLUSTER_KINDS: frozenset[str] = frozenset(
    {
        "ClusterRole",
        "ClusterRoleBinding",
        "CustomResourceDefinition",
        "IngressClass",
        "MutatingWebhookConfiguration",
        "Namespace",
        "Node",
        "PersistentVolume",
        "PriorityClass",
        "StorageClass",
        "ValidatingWebhookConfiguration",
    }
)
# Exclusive-tag scopes (`<scope>:<name>`) that name no cluster object:
#   order    - injection-order constraints between draws;
#   hostPort - a node port held by pods of the app namespace, released with them;
#   state    - in-namespace application state (e.g. a broker's queue contents).
# Any other scope is read as a kind name (`ResourceQuota:memory`, `namespace:policy-system`,
# `webhook:<name>` -> a webhook configuration, which is cluster-scoped).
LOCAL_TAG_SCOPES: frozenset[str] = frozenset({"order", "hostPort", "state"})
_TAG_SCOPE_ALIASES = {"webhook": "ValidatingWebhookConfiguration"}
_KINDS_BY_LOWER = {k.lower(): k for k in NAMESPACED_KINDS | CLUSTER_KINDS}


class Basis(enum.Enum):
    NAMESPACE_ONLY = "namespace_only"  # skipped: the namespace teardown removes every write
    NO_DRAW = "no_draw"  # legacy entry: nothing declared, recover
    HOST_LEVEL = "host_level"  # khaos / node-level fault, recover
    CLUSTER_FOOTPRINT = "cluster_footprint"
    CLUSTER_TAG = "cluster_tag"
    UNCLASSIFIED = "unclassified"  # unknown kind or tag scope: recover (fail closed)


@dataclass(frozen=True)
class Decision:
    recover: bool
    basis: Basis
    reason: str


def _kind_scope(kind: str, name: str, app_namespaces: frozenset[str]) -> Basis:
    if kind in NAMESPACED_KINDS or (kind == "Namespace" and name in app_namespaces):
        return Basis.NAMESPACE_ONLY
    if kind in CLUSTER_KINDS:
        return Basis.CLUSTER_FOOTPRINT
    return Basis.UNCLASSIFIED


def _tag_scope(tag: str, app_namespaces: frozenset[str]) -> Basis:
    scope, sep, name = tag.partition(":")
    if not sep:
        return Basis.UNCLASSIFIED
    if scope in LOCAL_TAG_SCOPES:
        return Basis.NAMESPACE_ONLY
    kind = _TAG_SCOPE_ALIASES.get(scope) or _KINDS_BY_LOWER.get(scope.lower())
    if kind is None:
        return Basis.UNCLASSIFIED
    basis = _kind_scope(kind, name, app_namespaces)
    return Basis.CLUSTER_TAG if basis is Basis.CLUSTER_FOOTPRINT else basis


def decide(draw: FaultDraw | None, app_namespaces: Iterable[str], *, host_level: bool = False) -> Decision:
    """Whether a composite must run this fault's `recover_fault` before the teardown.

    `app_namespaces` are the names the app namespace goes by (the canonical name footprints use
    and the session-suffixed name, if any). `host_level` is the problem's `requires_khaos()`.
    """
    if draw is None:
        return Decision(True, Basis.NO_DRAW, "no FaultDraw: writes undeclared")
    if host_level:
        return Decision(True, Basis.HOST_LEVEL, "host-level (khaos) fault")
    namespaces = frozenset(app_namespaces)
    for fp in draw.footprints:
        basis = _kind_scope(fp.kind, fp.name, namespaces)
        if basis is not Basis.NAMESPACE_ONLY:
            return Decision(True, basis, f"footprint {fp.kind}/{fp.name} is {basis.value}")
    for tag in sorted(draw.exclusive_tags):
        basis = _tag_scope(tag, namespaces)
        if basis is not Basis.NAMESPACE_ONLY:
            return Decision(True, basis, f"exclusive tag {tag!r} is {basis.value}")
    return Decision(False, Basis.NAMESPACE_ONLY, "every write is in the app namespace; its teardown removes it")
