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
"""Every catalog family names footprint fields one way, so overlaps across families are detected.

Convention (documented on `spec.Footprint`): a dotted Kubernetes JSON path with no list indices
and no container names, e.g. `spec.template.spec.containers.env.CART_ADDR` or
`spec.ports.targetPort`; `field=None` means the whole object. Map keys (env var names,
ConfigMap data keys, flag names) are path segments as written.
"""

import random
import re

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.spec import conflicts

# Segments of letters, digits, '_' and '-' joined by dots; the first starts with a letter.
_FIELD = re.compile(r"[A-Za-z][A-Za-z0-9_-]*(\.[A-Za-z0-9_-]+)*")
_LIST_INDEX = re.compile(r"(^|\.)[0-9]+(\.|$)")
# Fields of a Kubernetes Container. The segment after `containers.` must be one of these, which
# rules out a container name sneaking into the path.
_CONTAINER_FIELDS = frozenset(
    {
        "args",
        "command",
        "env",
        "envFrom",
        "image",
        "imagePullPolicy",
        "lifecycle",
        "livenessProbe",
        "ports",
        "readinessProbe",
        "resources",
        "securityContext",
        "startupProbe",
        "stdin",
        "terminationMessagePath",
        "tty",
        "volumeDevices",
        "volumeMounts",
        "workingDir",
    }
)
_CONTAINER_LISTS = ("containers", "initContainers")
DRAWS_PER_APP = 20
ENTRIES = {e.key: e for e in catalog.entries()}


def _draws(entry, app):
    rng = random.Random(f"{entry.key}|{app}")
    return [entry.sample(app, rng) for _ in range(DRAWS_PER_APP)]


@pytest.mark.parametrize(
    ("key", "app"), [(e.key, app) for e in catalog.entries() for app in sorted(e.apps)], ids=lambda v: str(v)
)
def test_every_footprint_field_follows_the_convention(key, app):
    for draw in _draws(ENTRIES[key], app):
        for fp in draw.footprints:
            if fp.field is None:
                continue
            assert _FIELD.fullmatch(fp.field), f"{key}: {fp} is not a dotted path"
            assert not _LIST_INDEX.search(fp.field), f"{key}: {fp} has a list index"
            segments = fp.field.split(".")
            for i, segment in enumerate(segments[:-1]):
                if segment in _CONTAINER_LISTS:
                    assert segments[i + 1] in _CONTAINER_FIELDS, f"{key}: {fp} names a container"


def _find(key, app, service):
    """A draw of `key` for `app` that writes the Deployment `service` (searching rng seeds)."""
    for seed in range(2000):
        draw = ENTRIES[key].sample(app, random.Random(seed))
        if any(fp.kind == "Deployment" and fp.name == service for fp in draw.footprints):
            return draw
    raise AssertionError(f"no draw of {key} on {service}")


# Known overlaps whose two faults come from different families.
_CROSS_FAMILY_OVERLAPS = [
    # delete-and-reapply whole Deployment (k8s_objects) vs a probe field (otel_flags)
    ("readiness_probe_misconfiguration", "astronomy_shop_failed_readiness_probe", "astronomy_shop", "cart"),
    # delete-and-reapply whole Deployment (k8s_objects) vs a container image (app_level)
    ("liveness_probe_misconfiguration", "incorrect_image", "social_network", "media-service"),
    # an env var field (app_level) vs a delete-and-reapply whole Deployment (k8s_objects)
    ("missing_env_variable", "resource_request_too_large", "astronomy_shop", "frontend"),
    # whole `search` Deployment (admission) vs a dnsPolicy field (k8s_objects)
    ("namespace_memory_limit", "wrong_dns_policy", "hotel_reservation", "search"),
]


@pytest.mark.parametrize(("a", "b", "app", "service"), _CROSS_FAMILY_OVERLAPS, ids=lambda v: str(v))
def test_known_cross_family_overlaps_conflict(a, b, app, service):
    assert conflicts(_find(a, app, service), _find(b, app, service))


def test_the_same_pairs_on_different_services_compose():
    assert not conflicts(
        _find("liveness_probe_misconfiguration", "social_network", "media-service"),
        _find("incorrect_image", "social_network", "text-service"),
    )
