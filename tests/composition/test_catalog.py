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
"""Contract checks for every catalog entry, whichever family module defines it."""

import json
import random

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.generator import draw_plan
from sregym.conductor.problems.composition.spec import CatalogEntry, Tier, conflicts

FOUR_APPS = frozenset({"hotel_reservation", "social_network", "astronomy_shop", "train_ticket"})
ENTRIES = catalog.entries()
APP_ENTRY_PAIRS = [(entry, app) for entry in ENTRIES for app in sorted(entry.apps)]


def _id(pair):
    entry, app = pair
    return f"{entry.key}@{app}"


def test_keys_are_unique_and_sorted():
    keys = [e.key for e in ENTRIES]
    assert keys == sorted(set(keys))
    assert all(isinstance(e, CatalogEntry) for e in ENTRIES)


@pytest.mark.parametrize("entry", ENTRIES, ids=lambda e: e.key)
def test_apps_are_a_non_empty_subset_of_the_four_apps(entry):
    assert entry.apps
    assert entry.apps <= FOUR_APPS


@pytest.mark.parametrize("pair", APP_ENTRY_PAIRS, ids=_id)
def test_sample_is_deterministic_and_well_formed(pair):
    entry, app = pair
    for seed in range(25):
        draw = entry.sample(app, random.Random(seed))
        assert draw == entry.sample(app, random.Random(seed))
        assert (draw.key, draw.app) == (entry.key, app)
        assert draw.footprints
        assert isinstance(draw.tier, Tier)
        json.dumps(draw.to_json())
        # A draw always conflicts with itself, so the generator never draws one fault twice.
        assert conflicts(draw, draw)


def test_entries_for_and_apps_agree_with_entries():
    for app in FOUR_APPS:
        assert catalog.entries_for(app) == tuple(e for e in ENTRIES if app in e.apps)
    assert catalog.apps() == frozenset(a for e in ENTRIES for a in e.apps)


@pytest.mark.parametrize("app", sorted(catalog.apps()))
def test_real_catalog_plans_are_conflict_free(app):
    for seed in range(40):
        plan = draw_plan(app, 3, seed, catalog.entries_for(app))
        for i, a in enumerate(plan.draws):
            assert all(not conflicts(a, b) for b in plan.draws[i + 1 :])
