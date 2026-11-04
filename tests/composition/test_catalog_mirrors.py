"""The pure catalog keeps copies of problem-module constants it may not import; they must match,
and component draws carry honest footprints and read dependencies."""

import random

import pytest

from sregym.conductor.problems import memcached_poisoned_entries, mongo_component_faults
from sregym.conductor.problems.composition import app_bindings, catalog
from sregym.conductor.problems.composition.catalog import component_mongo, component_state
from sregym.conductor.problems.composition.generator import PlanRef, plan_for
from sregym.conductor.problems.composition.spec import conflicts
from sregym.generators.fault.components import memcached, mongo


@pytest.mark.parametrize(
    "mirror,source",
    [
        (component_state.CORRUPTIONS, memcached.CORRUPTION_MODES),
        (component_state.KEY_SCOPES, memcached_poisoned_entries.KEY_SCOPES),
        (component_mongo.WRITE_MODES, mongo_component_faults.WRITE_MODES),
        (component_mongo.POISON_SHAPES, mongo.POISON_SHAPES),
        (component_mongo.POISON_SCOPES, mongo_component_faults.POISON_SCOPES),
    ],
)
def test_mirrors_match_their_sources(mirror, source):
    assert tuple(mirror) == tuple(source)


COMPONENT_KEYS = set(component_state.ENABLED_APPS) | set(component_mongo.ENABLED_APPS)


def test_a_fault_that_flushes_a_cache_declares_it():
    entries = {e.key: e for e in catalog.entries_for("social_network")}
    for seed in range(40):
        for key in ("mongo_collection_renamed", "mongo_poison_document"):
            draw = entries[key].sample("social_network", random.Random(seed))
            params = draw.param_dict()
            inst, coll = mongo_component_faults.resolve("social_network", params["target"], params["collection"])
            names = {f.name for f in draw.footprints}
            assert inst.workload in names and (coll.cache is None or coll.cache in names)


def test_plans_never_pair_a_restarting_fault_with_one_reading_through_that_store():
    depends = set(app_bindings.SOCIAL_NETWORK.integrity.depends_on)
    entries = catalog.entries_for("social_network")
    for seed in range(150):
        plan = plan_for(PlanRef("social_network", 4, seed, generation=2), entries)
        for a in plan.draws:
            for b in plan.draws:
                if a is b or not b.reads:
                    continue
                touched = {f.name for f in a.footprints} & depends
                assert not (a.may_restart and touched), (a.to_json(), b.to_json())
        caches = {c for d in plan.draws if d.key in COMPONENT_KEYS for c in (f.name for f in d.footprints)}
        assert len(caches) == sum(len(d.footprints) for d in plan.draws if d.key in COMPONENT_KEYS)


def test_a_store_restarting_draw_conflicts_with_a_seeded_data_reader():
    entries = {e.key: e for e in catalog.entries_for("social_network")}
    reader = entries["memcached_poisoned_entries"].sample("social_network", random.Random(5))
    assert reader.reads
    from sregym.conductor.problems.composition.spec import FaultDraw, Footprint

    restart = FaultDraw("k", "social_network", (), (Footprint("Deployment", "post-storage-mongodb", "spec.replicas"),))
    assert conflicts(restart, reader) and conflicts(reader, restart)
