"""Semantic invariants of generated social_network plans, stated independently of `spec.conflicts`.

The conflict rules decide which draws compose; these tests recheck the outcome over many seeds in
terms of what a plan must never contain, so a new fault kind that declares thin footprints (a
store it flushes, a store its verdict reads) is caught even if `conflicts` happens to accept it.
"""

from __future__ import annotations

import itertools

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.app_bindings import BINDINGS
from sregym.conductor.problems.composition.generator import PlanRef, plan_for

APP = "social_network"
SEEDS = range(250)


def _stores() -> set[str]:
    stores = {r.workload for rows in (BINDINGS[APP].redis, BINDINGS[APP].memcached, BINDINGS[APP].mongo) for r in rows}
    assert len(stores) >= 10, stores  # a guard: an empty set would make every test below vacuous
    return stores


def _plans(n: int, reliable: bool):
    entries = catalog.entries_for(APP)
    for seed in SEEDS:
        yield seed, plan_for(PlanRef(APP, n, seed, None, reliable, 2), entries)


@pytest.mark.parametrize("n", [2, 3])
@pytest.mark.parametrize("reliable", [False, True])
def test_no_store_is_touched_by_two_faults(n, reliable):
    """Two faults never name the same store workload in any footprint kind (Deployment, Service,
    ConfigMap, PVC, ...): the state of a store is shared by everything that touches it."""
    stores = _stores()
    for seed, plan in _plans(n, reliable):
        owners: dict[str, int] = {}
        for i, draw in enumerate(plan.draws):
            for name in {f.name for f in draw.footprints if f.name in stores}:
                assert owners.setdefault(name, i) == i, (seed, name, [d.key for d in plan.draws])


@pytest.mark.parametrize("n", [2, 3])
def test_no_fault_may_restart_a_store_another_verdict_reads_through(n):
    """A draw that may restart a workload (every draw without data checks of its own, and a draw
    that declares `restarts`) never has a footprint in another draw's `reads`."""
    for seed, plan in _plans(n, False):
        for a, b in itertools.permutations(plan.draws, 2):
            if a.may_restart:
                hit = {f.name for f in a.footprints} & b.reads
                assert not hit, (seed, a.key, b.key, hit)


@pytest.mark.parametrize("n", [2, 3])
def test_flushed_caches_belong_to_one_fault(n):
    """A cache that one fault flushes (it is in that draw's footprints without being its target)
    is not the target or a flushed cache of another draw."""
    for seed, plan in _plans(n, False):
        names = [{f.name for f in d.footprints if f.name in _stores()} for d in plan.draws]
        for (i, a), (j, b) in itertools.combinations(enumerate(names), 2):
            assert not a & b, (seed, plan.draws[i].key, plan.draws[j].key, a & b)
