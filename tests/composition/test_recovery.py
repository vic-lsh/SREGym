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
"""Which composite faults the namespace teardown removes (composition/recovery.py)."""

import random

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.catalog._kit import APP_NAMESPACES  # names footprints use
from sregym.conductor.problems.composition.recovery import CLUSTER_KINDS, NAMESPACED_KINDS, Basis, decide
from sregym.conductor.problems.composition.spec import FaultDraw, Footprint

NS = "hotel-reservation"
NAMESPACES = {NS, f"{NS}-session1"}


def _draw(*footprints, tags=()):
    return FaultDraw(key="k", app="hotel_reservation", params=(), footprints=footprints, exclusive_tags=frozenset(tags))


@pytest.mark.parametrize(
    "draw",
    [
        _draw(Footprint("Service", "user", "spec.ports.targetPort")),
        _draw(Footprint("Deployment", "geo"), Footprint("ConfigMap", "c"), Footprint("NetworkPolicy", "deny")),
        _draw(Footprint("Namespace", NS, "metadata.labels")),  # PSA labels on the app namespace
        _draw(Footprint("ResourceQuota", "q"), tags={"ResourceQuota:memory"}),
        _draw(Footprint("Deployment", "kafka"), tags={"state:astronomy_shop/kafka-broker", "order:a-before-b"}),
        _draw(Footprint("Deployment", "x"), tags={"hostPort:9100"}),
    ],
)
def test_namespace_only_faults_are_skipped(draw):
    decision = decide(draw, NAMESPACES)

    assert not decision.recover and decision.basis is Basis.NAMESPACE_ONLY


@pytest.mark.parametrize(
    ("draw", "basis"),
    [
        (_draw(Footprint("Service", "u"), Footprint("ClusterRole", "r")), Basis.CLUSTER_FOOTPRINT),
        (_draw(Footprint("MutatingWebhookConfiguration", "w")), Basis.CLUSTER_FOOTPRINT),
        (_draw(Footprint("PriorityClass", "high")), Basis.CLUSTER_FOOTPRINT),  # reconcile never deletes these
        (_draw(Footprint("Node", "worker-1", "metadata.labels")), Basis.CLUSTER_FOOTPRINT),  # pre-existing object
        (_draw(Footprint("Namespace", "policy-system")), Basis.CLUSTER_FOOTPRINT),
        (_draw(Footprint("Namespace", "ingress-nginx")), Basis.CLUSTER_FOOTPRINT),  # reconcile skips it
        (_draw(Footprint("Service", "u"), tags={"namespace:policy-system"}), Basis.CLUSTER_TAG),
        (_draw(Footprint("Service", "u"), tags={"webhook:pod-policy.validation.k8s.io"}), Basis.CLUSTER_TAG),
        (_draw(Footprint("Gizmo", "g")), Basis.UNCLASSIFIED),
        (_draw(Footprint("Service", "u"), tags={"mystery:x"}), Basis.UNCLASSIFIED),
        (_draw(Footprint("Service", "u"), tags={"no-scope"}), Basis.UNCLASSIFIED),
    ],
)
def test_cluster_scoped_or_unclassified_faults_are_recovered(draw, basis):
    decision = decide(draw, NAMESPACES)

    assert decision.recover and decision.basis is basis


def test_legacy_and_host_level_faults_are_recovered():
    assert decide(None, NAMESPACES).basis is Basis.NO_DRAW
    decision = decide(_draw(Footprint("Service", "u")), NAMESPACES, host_level=True)
    assert decision.recover and decision.basis is Basis.HOST_LEVEL


def test_session_suffixed_namespace_counts_as_the_app_namespace():
    assert not decide(_draw(Footprint("Namespace", f"{NS}-session1")), NAMESPACES).recover


def test_kind_lists_are_disjoint():
    assert not NAMESPACED_KINDS & CLUSTER_KINDS


def _catalog_draws():
    rng = random.Random(0)
    for entry in catalog.entries():
        for app in sorted(entry.apps):
            for _ in range(20):
                yield app, entry.sample(app, rng)


CATALOG_DRAWS = list(_catalog_draws())


def test_every_catalog_draw_is_classified():
    """Fail closed is only a safety net: a new kind or tag scope must be classified."""
    unclassified = {
        (d.key, decide(d, {APP_NAMESPACES[app]}).reason)
        for app, d in CATALOG_DRAWS
        if decide(d, {APP_NAMESPACES[app]}).basis is Basis.UNCLASSIFIED
    }
    assert not unclassified


def test_adding_a_cluster_footprint_to_any_catalog_draw_forces_recovery():
    for app, d in CATALOG_DRAWS:
        widened = FaultDraw(
            key=d.key,
            app=d.app,
            params=d.params,
            footprints=d.footprints + (Footprint("ClusterRole", "extra"),),
            tier=d.tier,
            exclusive_tags=d.exclusive_tags,
        )
        assert decide(widened, {APP_NAMESPACES[app]}).recover


def test_catalog_has_both_skippable_and_recovered_faults():
    recovered = {d.key for app, d in CATALOG_DRAWS if decide(d, {APP_NAMESPACES[app]}).recover}
    skipped = {d.key for app, d in CATALOG_DRAWS if not decide(d, {APP_NAMESPACES[app]}).recover}

    assert {"admission_webhook_outage", "admission_webhook_tls_mismatch"} <= recovered
    assert {"psa_restricted_blocks_recreation", "namespace_memory_limit"} <= skipped
