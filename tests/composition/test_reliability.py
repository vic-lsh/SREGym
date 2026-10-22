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
"""--exclude-unreliable-faults: the `__reliable` plan ids and the flag table.

Default ids must keep drawing exactly the plans they drew before the flag existed; the
`__reliable` variant must be deterministic and never draw a flagged (entry, app).
"""

import itertools

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.generator import (
    PlanRef,
    draw_plan,
    parse_plan_id,
    plan_for,
    plan_id,
)
from sregym.conductor.problems.composition.reliability import UNRELIABLE, reliable_entries
from sregym.conductor.problems.composition.schedule import DEFAULT_APPS, plan_sequence

SEEDS = range(40)
NS = (2, 3, 5, 8)


def _keys(plan) -> list[str]:
    return [draw.key for draw in plan.draws]


def test_every_flag_names_a_catalog_entry_for_its_app_and_gives_a_reason() -> None:
    by_key = {entry.key: entry for entry in catalog.entries()}
    for (key, app), reason in UNRELIABLE.items():
        assert key in by_key, key
        assert app in by_key[key].apps, (key, app)
        assert len(reason) > 40, (key, app)


def test_the_nested_experiment_base_plan_is_unchanged() -> None:
    plan = plan_for(parse_plan_id("mf__hotel_reservation__n8__s161__v_"), catalog.entries_for("hotel_reservation"))
    assert _keys(plan) == [
        "missing_service",
        "service_wrong_pod_selection_hotel_reservation",
        "resource_request_too_small",
        "liveness_probe_misconfiguration",
        "incorrect_image",
        "wrong_service_selector",
        "update_incompatible_correlated",
        "network_policy_block",
    ]


@pytest.mark.parametrize("app", DEFAULT_APPS)
def test_default_ids_draw_from_the_whole_catalog(app: str) -> None:
    entries = catalog.entries_for(app)
    for n, seed in itertools.product(NS, SEEDS):
        assert plan_for(PlanRef(app, n, seed), entries) == draw_plan(app, n, seed, entries)


@pytest.mark.parametrize("app", DEFAULT_APPS)
def test_reliable_ids_never_draw_a_flagged_entry_and_are_deterministic(app: str) -> None:
    entries = catalog.entries_for(app)
    for n, seed in itertools.product(NS, SEEDS):
        ref = parse_plan_id(plan_id(app, n, seed, reliable=True))
        plan = plan_for(ref, entries)
        assert all((key, app) not in UNRELIABLE for key in _keys(plan))
        assert plan == plan_for(ref, list(reversed(entries)))  # entry order does not matter
        assert plan == draw_plan(app, n, seed, reliable_entries(app, entries))


def test_the_flag_changes_plans_that_drew_a_flagged_entry() -> None:
    entries = catalog.entries_for("hotel_reservation")
    default = plan_for(PlanRef("hotel_reservation", 8, 161), entries)
    reliable = plan_for(PlanRef("hotel_reservation", 8, 161, reliable=True), entries)
    assert _keys(default) != _keys(reliable)
    assert {"missing_service", "resource_request_too_small", "wrong_service_selector"} <= set(_keys(default))


def test_apps_without_flags_draw_the_same_plans_either_way() -> None:
    flagged_apps = {app for _, app in UNRELIABLE}
    for app in sorted(set(DEFAULT_APPS) - flagged_apps):
        entries = catalog.entries_for(app)
        for n, seed in itertools.product(NS, range(10)):
            assert (
                plan_for(PlanRef(app, n, seed, reliable=True), entries).draws
                == plan_for(PlanRef(app, n, seed), entries).draws
            )


@pytest.mark.parametrize("take", [None, 3])
def test_reliable_ids_round_trip_and_compose_with_take(take: int | None) -> None:
    pid = plan_id("hotel_reservation", 5, 7, take, reliable=True)
    assert pid.endswith("__reliable__v_")
    assert parse_plan_id(pid) == PlanRef("hotel_reservation", 5, 7, take, True)
    assert parse_plan_id(pid.replace("__reliable", "")) == PlanRef("hotel_reservation", 5, 7, take, False)


def test_the_suffix_must_follow_take() -> None:
    assert parse_plan_id("mf__hotel_reservation__n5__s7__reliable__k3__v_") is None


def test_the_schedule_marks_every_composition_and_keeps_the_slots() -> None:
    weights = {1: 1.0, 2: 1.0, 5: 1.0}
    singles = (f"single_{i}" for i in itertools.count())
    default = plan_sequence(60, weights, DEFAULT_APPS, 3, singles)
    singles = (f"single_{i}" for i in itertools.count())
    reliable = plan_sequence(60, weights, DEFAULT_APPS, 3, singles, reliable=True)
    assert len(default) == len(reliable)
    for before, after in zip(default, reliable, strict=True):
        if before.startswith("mf__"):
            assert after == before.replace("__v_", "__reliable__v_")
        else:
            assert after == before
