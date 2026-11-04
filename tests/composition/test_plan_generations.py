"""Catalog generations keep existing plan ids drawing exactly what they drew before.

Fixtures:
- social_network_g1_plans.json: plain, take-K and __reliable social-network ids drawn by the
  catalog at a1176f19 (before any component incident fault existed).
- mixed10_s42_plans.json: every id of the mixed10 stream_s42_single50_multi50 experiment and
  the faults it drew.
Generation 1 is frozen; its key set is pinned here. Adding an entry (or an app to an entry) at
generation 1 fails; new families enter at an open generation, reachable only through `__g<G>`.
"""

import json
from pathlib import Path

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.generator import PlanRef, parse_plan_id, plan_for, plan_id
from sregym.conductor.problems.composition.spec import FROZEN_GENERATION, LATEST_GENERATION

FIXTURES = Path(__file__).parent / "fixtures"
SOCIAL = json.loads((FIXTURES / "social_network_g1_plans.json").read_text())
MIXED10 = json.loads((FIXTURES / "mixed10_s42_plans.json").read_text())

INCIDENT_KEYS = {
    "redis_replica_misconfig",
    "memcached_poisoned_entries",
    "mongo_write_lock",
    "mongo_write_rejection",
    "mongo_collection_renamed",
    "mongo_poison_document",
}
INFRASTRUCTURE_KEYS = {"process_frozen", "pod_network_impairment", "connection_exhaustion", "traffic_surge"}


def _draws(pid: str) -> list[dict]:
    ref = parse_plan_id(pid)
    plan = plan_for(ref, catalog.entries_for(ref.app))
    return [{"key": d.key, "params": d.param_dict(), "tier": d.tier.name} for d in plan.draws]


@pytest.mark.parametrize("pid", sorted(SOCIAL))
def test_social_network_ids_draw_as_before_the_incident_families(pid):
    assert _draws(pid) == SOCIAL[pid]


@pytest.mark.parametrize("pid", sorted(MIXED10))
def test_mixed10_ids_draw_what_the_experiment_drew(pid):
    assert _draws(pid) == MIXED10[pid]


def test_generation_one_key_sets_are_frozen():
    by_app = {
        app: sorted(e.key for e in catalog.entries_for(app) if e.generation(app) <= FROZEN_GENERATION)
        for app in sorted(catalog.apps())
    }
    pinned = json.loads((FIXTURES / "catalog_generation1_keys.json").read_text())
    assert by_app == pinned


def test_incident_families_are_reachable_only_through_a_generation_segment():
    for entry in catalog.entries():
        for app in entry.apps:
            want = 3 if entry.key in INFRASTRUCTURE_KEYS else 2 if entry.key in INCIDENT_KEYS else 1
            assert entry.generation(app) == want, (entry.key, app)
    entries = catalog.entries_for("social_network")

    def keys(generation):
        return {
            d.key
            for seed in range(60)
            for d in plan_for(PlanRef("social_network", 3, seed, generation=generation), entries).draws
        }

    assert keys(2) & INCIDENT_KEYS and not keys(2) & INFRASTRUCTURE_KEYS
    assert keys(3) & INFRASTRUCTURE_KEYS


@pytest.mark.parametrize("reliable", [False, True])
@pytest.mark.parametrize("take", [None, 2])
def test_generation_segment_round_trips(take, reliable):
    pid = plan_id("social_network", 4, 9, take=take, reliable=reliable, generation=2)
    assert pid.endswith("__g2__v_")
    assert parse_plan_id(pid) == PlanRef("social_network", 4, 9, take, reliable, 2)


@pytest.mark.parametrize(
    "pid",
    [
        "mf__social_network__n3__s1__g1__v_",  # generation 1 is spelled without a segment
        f"mf__social_network__n3__s1__g{LATEST_GENERATION + 1}__v_",
        "mf__social_network__n3__s1__g2__reliable__v_",  # order: reliable before generation
    ],
)
def test_bad_generation_segments_are_not_plan_ids(pid):
    assert parse_plan_id(pid) is None
