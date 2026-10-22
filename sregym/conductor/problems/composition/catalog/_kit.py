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
"""Shared helpers for the app-level catalog families (app_level, otel_flags, admission).

Standard library only. Defines no faults (``ENTRIES`` is empty) so catalog discovery may import it
like any other submodule.

Footprint field convention (shared with every family so prefix overlap works across modules):
Kubernetes JSON paths joined with dots, list indices and container names dropped, e.g.
``spec.template.spec.containers.image`` or ``spec.template.spec.containers.env.CART_ADDR``.
A fault whose liveness lives in a running pod (in-memory or in-DB state that a restart heals or
wipes) declares the whole Deployment (``field=None``) so that any other fault touching that
Deployment conflicts with it.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint, Tier

ENTRIES: tuple[CatalogEntry, ...] = ()

ALL_APPS = frozenset({"hotel_reservation", "social_network", "astronomy_shop", "train_ticket"})

# Mirrors of `variant_specs._REPRESENTATIVE_SERVICES` and `variant_utils.SERVICES_BY_APP` (those
# modules import the apps, so the pure catalog cannot). Enforced by
# tests/composition/test_catalog_app_sources.py, which parses the originals.
REPRESENTATIVE_SERVICES: dict[str, tuple[str, ...]] = {
    "social_network": (
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
    "hotel_reservation": ("frontend", "geo", "profile", "rate", "recommendation", "search", "reservation", "user"),
    "astronomy_shop": (
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
    "train_ticket": (
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

HOTEL_DEPLOYMENTS: tuple[str, ...] = (
    "frontend",
    "geo",
    "profile",
    "rate",
    "recommendation",
    "search",
    "reservation",
    "user",
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
    "mongodb-user",
    "memcached-profile",
    "memcached-rate",
    "memcached-reservation",
)

# App namespaces (`service/metadata/<app>.json`).
APP_NAMESPACES: dict[str, str] = {
    "hotel_reservation": "hotel-reservation",
    "social_network": "social-network",
    "astronomy_shop": "astronomy-shop",
    "train_ticket": "train-ticket",
}


@dataclass(frozen=True)
class Pick:
    """What one sampled fault instance touches and how it is ordered."""

    params: dict[str, Any]
    footprints: tuple[Footprint, ...]
    tier: Tier = Tier.ORDINARY
    tags: frozenset[str] = field(default_factory=frozenset)


Choose = Callable[[str, random.Random], Pick]
Build = Callable[[str, dict[str, Any]], Any]


def entry(key: str, apps: Iterable[str], choose: Choose, build: Build) -> CatalogEntry:
    """Wrap a pure `choose(app, rng)` and a lazy `build(app, params)` into a CatalogEntry.

    `sample` raises ValueError for an app outside `apps`, and for a pick without footprints.
    """
    supported = frozenset(apps)
    unknown = supported - ALL_APPS
    if unknown:
        raise ValueError(f"{key}: unknown apps {sorted(unknown)}")

    def sample(app: str, rng: random.Random) -> FaultDraw:
        if app not in supported:
            raise ValueError(f"{key} does not support app {app!r}; supported: {sorted(supported)}")
        pick = choose(app, rng)
        if not pick.footprints:
            raise ValueError(f"{key}: a draw must declare at least one footprint")
        return FaultDraw(
            key=key,
            app=app,
            params=tuple(sorted(pick.params.items())),
            footprints=pick.footprints,
            tier=pick.tier,
            exclusive_tags=pick.tags,
        )

    return CatalogEntry(key=key, apps=supported, sample=sample, build=lambda draw: build(draw.app, draw.param_dict()))


def fixed(pick: Pick) -> Choose:
    """A `choose` for a parameterless fault: always the same pick."""
    return lambda app, rng: pick


def deployment(name: str, path: str | None = None) -> Footprint:
    return Footprint("Deployment", name, path)


def container(name: str, path: str) -> Footprint:
    """A field of the Deployment's pod-template containers, e.g. `container("ad", "image")`."""
    return Footprint("Deployment", name, f"spec.template.spec.containers.{path}")
