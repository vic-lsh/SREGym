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
"""Value types for multi-fault composition: what a fault touches and how draws conflict.

Pure module: standard library only. A `CatalogEntry` describes one fault class; its
`sample` picks concrete parameters for an app and returns a `FaultDraw`, whose
footprints declare every Kubernetes object (or field) the injector writes.
Two draws conflict when any footprints overlap or they share an exclusive tag.
"""

from __future__ import annotations

import enum
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


def _is_dotted_prefix(prefix: str, path: str) -> bool:
    return path == prefix or path.startswith(prefix + ".")


@dataclass(frozen=True)
class Footprint:
    """One Kubernetes object, or one field of it, that an injector writes.

    `kind` is the Kubernetes kind ("Deployment", "Service", ...); `name` is the object
    name (namespace implied: the app namespace, or cluster scope for cluster kinds);
    `field` is a dotted path such as "spec.ports.targetPort", or None for the whole object.

    One field convention for every catalog family, so overlaps across families are found:
    the Kubernetes JSON path joined with dots, without list indices or container names
    (`spec.template.spec.containers.env.CART_ADDR`, not `...containers[0]...` or
    `...containers.frontend.env...`). Map keys (env names, ConfigMap data keys) are segments
    as written. Enforced by tests/composition/test_footprint_convention.py.
    """

    kind: str
    name: str
    field: str | None = None

    def overlaps(self, other: Footprint) -> bool:
        """Same kind and name, and either field is None, equal, or one is a dotted prefix of the other."""
        if self.kind != other.kind or self.name != other.name:
            return False
        if self.field is None or other.field is None:
            return True
        return _is_dotted_prefix(self.field, other.field) or _is_dotted_prefix(other.field, self.field)


class Tier(enum.IntEnum):
    """Injection order class. Lower tiers are injected first."""

    RESTARTS_PODS = 0  # injector deletes/recreates pods or workloads: inject first
    ORDINARY = 1
    BLOCKS_ADMISSION = 2  # webhooks, PSA labels, quotas/limits: block later pod creation, inject last


@dataclass(frozen=True)
class FaultDraw:
    """One concrete fault chosen for a multi-fault plan."""

    key: str  # CatalogEntry.key
    app: str  # SREGym app name: hotel_reservation | social_network | astronomy_shop | train_ticket
    params: tuple[tuple[str, Any], ...]  # sorted (name, value) pairs, JSON-serializable values
    footprints: tuple[Footprint, ...]
    tier: Tier = Tier.ORDINARY
    exclusive_tags: frozenset[str] = frozenset()  # two draws sharing a tag conflict (fixed-name resources)

    def __post_init__(self) -> None:
        names = [name for name, _ in self.params]
        if names != sorted(set(names)):
            raise ValueError(f"FaultDraw {self.key!r}: params must be sorted by unique name, got {names}")
        if not self.footprints:
            raise ValueError(f"FaultDraw {self.key!r}: footprints must not be empty")

    def param_dict(self) -> dict[str, Any]:
        return dict(self.params)

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "app": self.app,
            "params": self.param_dict(),
            "footprints": [{"kind": f.kind, "name": f.name, "field": f.field} for f in self.footprints],
            "tier": self.tier.name,
            "exclusive_tags": sorted(self.exclusive_tags),
        }


def conflicts(a: FaultDraw, b: FaultDraw) -> bool:
    """Any footprint overlap, or a shared exclusive tag."""
    if a.exclusive_tags & b.exclusive_tags:
        return True
    return any(fa.overlaps(fb) for fa in a.footprints for fb in b.footprints)


@dataclass(frozen=True)
class CatalogEntry:
    """A composable fault class.

    `sample` is PURE: it chooses params and footprints for `app` using only the given rng.
    `build` is HEAVY: it lazily imports the problem class and returns the sregym Problem.
    """

    key: str
    apps: frozenset[str]
    sample: Callable[[str, random.Random], FaultDraw]
    build: Callable[[FaultDraw], Any]
