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
"""Multi-fault plan generation (composition/generator.py), over a fake catalog."""

import itertools
import random

import pytest

from sregym.conductor.problems.composition.generator import (
    PLAN_ID_PREFIX,
    CompositionPlan,
    PlanError,
    PlanRef,
    draw_plan,
    parse_plan_id,
    parse_weights,
    plan_for,
    plan_id,
    sample_n,
    take_plan,
)
from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint, Tier, conflicts

APP = "hotel_reservation"
OTHER_APP = "social_network"
_SERVICES = tuple(f"svc{i}" for i in range(6))


def _entry(key, *, kind, field, tier=Tier.ORDINARY, tags=frozenset(), apps=(APP,), services=_SERVICES):
    """A fake entry that follows the contract: picks a service with rng only."""

    def sample(app, rng):
        service = rng.choice(services)
        return FaultDraw(
            key=key,
            app=app,
            params=(("faulty_service", service),),
            footprints=(Footprint(kind, service, field),),
            tier=tier,
            exclusive_tags=tags,
        )

    def build(draw):
        raise AssertionError("pure tests never build problems")

    return CatalogEntry(key=key, apps=frozenset(apps), sample=sample, build=build)


FAKE_CATALOG = (
    _entry("whole_deployment", kind="Deployment", field=None, tier=Tier.RESTARTS_PODS),
    _entry("probe", kind="Deployment", field="spec.template.spec.containers.readinessProbe"),
    _entry("selector", kind="Service", field="spec.selector"),
    _entry("ports", kind="Service", field="spec.ports.targetPort"),
    _entry("quota", kind="ResourceQuota", field=None, tier=Tier.BLOCKS_ADMISSION, tags=frozenset({"quota"})),
    _entry("other_app_only", kind="Service", field=None, apps=(OTHER_APP,)),
)


def _assert_valid(plan, app, n, seed):
    assert (plan.app, plan.n, plan.seed) == (app, n, seed)
    assert len(plan.draws) == n
    assert all(d.app == app for d in plan.draws)
    for i, a in enumerate(plan.draws):
        for b in plan.draws[i + 1 :]:
            assert not conflicts(a, b), (a, b)
    _assert_tier_ordered(plan.draws)


def _assert_tier_ordered(draws):
    tiers = [d.tier for d in draws]
    assert tiers == sorted(tiers)


@pytest.mark.parametrize("n", [1, 2, 4, 8])
def test_plans_never_conflict_and_are_tier_ordered(n):
    for seed in range(300):
        _assert_valid(draw_plan(APP, n, seed, FAKE_CATALOG), APP, n, seed)


def test_plan_is_deterministic_and_independent_of_entry_order():
    shuffled = list(FAKE_CATALOG)
    random.Random(0).shuffle(shuffled)
    for seed in range(50):
        assert draw_plan(APP, 5, seed, FAKE_CATALOG) == draw_plan(APP, 5, seed, shuffled)


def test_different_seeds_give_different_plans():
    plans = {draw_plan(APP, 4, seed, FAKE_CATALOG).draws for seed in range(30)}
    assert len(plans) > 1


def test_only_entries_for_the_app_are_drawn():
    for seed in range(100):
        assert all(d.key != "other_app_only" for d in draw_plan(APP, 6, seed, FAKE_CATALOG).draws)


def test_same_entry_may_be_drawn_twice_with_disjoint_params():
    single = (_entry("selector", kind="Service", field="spec.selector"),)
    plan = draw_plan(APP, 3, 1, single)
    assert [d.key for d in plan.draws] == ["selector"] * 3
    assert len({d.param_dict()["faulty_service"] for d in plan.draws}) == 3


def test_plan_error_when_no_valid_plan_exists():
    single = (_entry("selector", kind="Service", field="spec.selector"),)
    with pytest.raises(PlanError):
        draw_plan(APP, len(_SERVICES) + 1, 0, single)
    tagged = (_entry("quota", kind="ResourceQuota", field=None, tags=frozenset({"quota"})),)
    with pytest.raises(PlanError):
        draw_plan(APP, 2, 0, tagged)


@pytest.mark.parametrize("n", [0, -1])
def test_plan_error_for_non_positive_n(n):
    with pytest.raises(PlanError):
        draw_plan(APP, n, 0, FAKE_CATALOG)


def test_plan_error_for_unsupported_app():
    with pytest.raises(PlanError, match="train_ticket"):
        draw_plan("train_ticket", 1, 0, FAKE_CATALOG)


def test_plan_type():
    assert isinstance(draw_plan(APP, 1, 0, FAKE_CATALOG), CompositionPlan)


@pytest.mark.parametrize("app", ["hotel_reservation", "social_network", "astronomy_shop", "train_ticket"])
@pytest.mark.parametrize("n", [1, 5, 12])
@pytest.mark.parametrize("seed", [0, 1, 999, 2**40])
def test_plan_id_round_trips(app, n, seed):
    pid = plan_id(app, n, seed)
    assert pid.startswith(PLAN_ID_PREFIX)
    assert pid.endswith("__v_")
    assert parse_plan_id(pid) == PlanRef(app, n, seed)


def test_plan_id_format():
    assert plan_id("hotel_reservation", 5, 1234) == "mf__hotel_reservation__n5__s1234__v_"


@pytest.mark.parametrize(
    "problem_id",
    [
        "k8s_target_port-misconfig",
        "drift_targetport__n3__v_hotel_reservation",
        "rcomp__hotel_reservation__n5__s1__v_",
        "mf__hotel_reservation__n5__s1",
        "mf__hotel_reservation__n0__s1__v_",
        "mf__hotel_reservation__n05__s1__v_",
        "mf__hotel_reservation__n5__s-1__v_",
        "mf____n5__s1__v_",
        "mf__hotel__reservation__n5__s1__v_",
        "mf__hotel_reservation__n5__s1__v_extra",
        "mf__hotel_reservation__n5__s1__k0__v_",
        "mf__hotel_reservation__n5__s1__k6__v_",
        "mf__hotel_reservation__n5__s1__k03__v_",
        "mf__hotel_reservation__n5__s1__k-1__v_",
        "mf__hotel_reservation__n5__s1__k__v_",
        "mf__hotel_reservation__n5__k2__s1__v_",
    ],
)
def test_parse_plan_id_rejects_other_ids(problem_id):
    assert parse_plan_id(problem_id) is None


@pytest.mark.parametrize(("app", "n", "seed"), [("hotel__x", 1, 0), ("", 1, 0), (APP, 0, 0), (APP, 1, -1)])
def test_plan_id_rejects_unrepresentable_inputs(app, n, seed):
    with pytest.raises(ValueError):
        plan_id(app, n, seed)


@pytest.mark.parametrize("app", ["hotel_reservation", "train_ticket"])
@pytest.mark.parametrize(("n", "take"), [(1, 1), (8, 1), (8, 4), (8, 8), (12, 7)])
@pytest.mark.parametrize("seed", [0, 161, 2**40])
def test_take_plan_id_round_trips(app, n, take, seed):
    pid = plan_id(app, n, seed, take)
    assert pid.endswith(f"__k{take}__v_")
    assert parse_plan_id(pid) == PlanRef(app, n, seed, take)


def test_take_plan_id_format():
    assert plan_id("hotel_reservation", 8, 161, 4) == "mf__hotel_reservation__n8__s161__k4__v_"


@pytest.mark.parametrize("take", [0, -1, 9])
def test_plan_id_rejects_take_outside_one_to_n(take):
    with pytest.raises(ValueError):
        plan_id(APP, 8, 0, take)


def _take(n, seed, k):
    return take_plan(draw_plan(APP, n, seed, FAKE_CATALOG), k)


@pytest.mark.parametrize("seed", range(40))
def test_take_n_is_the_whole_base_plan(seed):
    base = draw_plan(APP, 6, seed, FAKE_CATALOG)
    whole = take_plan(base, 6)
    assert whole.draws == base.draws
    assert whole.base_index == tuple(range(6))
    assert (whole.app, whole.n, whole.seed, whole.take) == (APP, 6, seed, 6)


@pytest.mark.parametrize("seed", range(200))
def test_take_k_subsets_are_nested(seed):
    subsets = [_take(8, seed, k) for k in (1, 2, 4, 8)]
    for smaller, bigger in itertools.pairwise(subsets):
        assert set(smaller.base_index) < set(bigger.base_index)
    base = draw_plan(APP, 8, seed, FAKE_CATALOG)
    for sub in subsets:
        assert len(sub.draws) == sub.take
        assert sub.draws == tuple(base.draws[i] for i in sub.base_index)
        assert list(sub.base_index) == sorted(sub.base_index)  # base-plan order is tier order
        _assert_tier_ordered(sub.draws)


def test_take_one_is_not_always_the_first_draw():
    assert len({_take(8, seed, 1).base_index for seed in range(50)}) > 1


@pytest.mark.parametrize("k", [0, -1, 9])
def test_take_plan_rejects_k_outside_one_to_n(k):
    with pytest.raises(PlanError):
        _take(8, 0, k)


def test_take_plan_rejects_a_subset():
    with pytest.raises(PlanError):
        take_plan(_take(8, 0, 4), 2)


@pytest.mark.parametrize("seed", range(20))
def test_plan_for_follows_the_id(seed):
    assert plan_for(PlanRef(APP, 5, seed), FAKE_CATALOG) == draw_plan(APP, 5, seed, FAKE_CATALOG)
    assert plan_for(PlanRef(APP, 5, seed, 3), FAKE_CATALOG) == _take(5, seed, 3)


def test_parse_weights():
    assert parse_weights("1:3,2:2,3:2,5:1,8:1") == {1: 3.0, 2: 2.0, 3: 2.0, 5: 1.0, 8: 1.0}
    assert parse_weights(" 4 : 0.5 ") == {4: 0.5}


@pytest.mark.parametrize(
    "spec", ["", "1", "1:", ":1", "a:1", "1:a", "0:1", "-1:1", "1:0", "1:-2", "1:inf", "1:nan", "1:1,1:2", "1:1,"]
)
def test_parse_weights_rejects_invalid(spec):
    with pytest.raises(ValueError):
        parse_weights(spec)


def test_sample_n_is_deterministic_and_within_support():
    weights = parse_weights("1:3,2:2,5:1")
    draws_a = [sample_n(weights, random.Random(7)) for _ in range(5)]
    draws_b = [sample_n(weights, random.Random(7)) for _ in range(5)]
    assert draws_a == draws_b
    rng = random.Random(0)
    seen = {sample_n(weights, rng) for _ in range(500)}
    assert seen == {1, 2, 5}


def test_sample_n_rejects_empty_weights():
    with pytest.raises(ValueError):
        sample_n({}, random.Random(0))
