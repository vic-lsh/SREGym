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
"""Tests for the pure problem-id schedule (composition/schedule.py)."""

import itertools
from collections import Counter

import pytest

from sregym.conductor.problems.composition.generator import parse_plan_id
from sregym.conductor.problems.composition.schedule import (
    DEFAULT_APPS,
    STRATEGIES,
    filter_pool_by_apps,
    infer_app,
    plan_sequence,
    single_fault_stream,
    slot_seed,
)

WEIGHTS = {1: 3.0, 2: 2.0, 3: 2.0, 5: 1.0, 8: 1.0}
POOL = [
    "target_port",
    "incorrect_image__v_hotel_reservation_frontend_bad",
    "incorrect_image__v_astronomy_shop_cart_bad",
    "readiness_probe__v_social_network_1",
    "readiness_probe__v_train_ticket_3",
]


def _singles():
    return (f"single_{i}" for i in itertools.count())


def _plan(seed, num=200, weights=WEIGHTS, apps=DEFAULT_APPS, stream=None):
    return plan_sequence(num, weights, apps, seed, stream if stream is not None else _singles())


@pytest.mark.parametrize("seed", range(20))
def test_same_arguments_give_the_same_sequence(seed):
    assert _plan(seed) == _plan(seed)


def test_different_seeds_give_different_sequences():
    assert len({tuple(_plan(seed, num=30)) for seed in range(10)}) == 10


def test_fault_counts_follow_the_weights():
    sequence = _plan(seed=7, num=6000)
    counts = Counter(1 if parse_plan_id(pid) is None else parse_plan_id(pid).n for pid in sequence)
    total = sum(WEIGHTS.values())
    assert set(counts) <= set(WEIGHTS)
    for n, weight in WEIGHTS.items():
        # Loose bound: binomial std-dev at 6000 draws is under 0.007 for every share here.
        assert abs(counts[n] / len(sequence) - weight / total) < 0.03, (n, counts)


@pytest.mark.parametrize("seed", range(10))
def test_single_fault_slots_consume_the_stream_in_order(seed):
    sequence = _plan(seed)
    singles = [pid for pid in sequence if parse_plan_id(pid) is None]
    assert singles == [f"single_{i}" for i in range(len(singles))]


def test_single_fault_only_weights_return_the_stream_prefix():
    assert plan_sequence(4, {1: 1.0}, DEFAULT_APPS, 0, iter("abcdef")) == list("abcd")


def test_stream_is_not_consumed_without_single_fault_slots():
    stream = iter(["only"])
    sequence = plan_sequence(50, {2: 1.0, 3: 1.0}, DEFAULT_APPS, 3, stream)
    assert all(parse_plan_id(pid) is not None for pid in sequence)
    assert next(stream) == "only"


@pytest.mark.parametrize("apps", [("hotel_reservation",), ("astronomy_shop", "train_ticket")])
def test_composite_slots_use_only_the_requested_apps(apps):
    composites = [parse_plan_id(pid) for pid in _plan(seed=1, apps=apps)]
    drawn = {parsed.app for parsed in composites if parsed is not None}
    assert drawn == set(apps)


@pytest.mark.parametrize("seed", range(10))
def test_plan_ids_parse_back_to_slot_count_and_seed(seed):
    for slot, pid in enumerate(_plan(seed)):
        parsed = parse_plan_id(pid)
        if parsed is None:
            continue
        app, n, plan_seed = parsed.app, parsed.n, parsed.seed
        assert parsed.take is None
        assert app in DEFAULT_APPS
        assert n in WEIGHTS and n >= 2
        assert plan_seed == slot_seed(seed, slot)


def test_app_order_does_not_change_the_sequence():
    assert _plan(5, apps=["train_ticket", "hotel_reservation"]) == _plan(5, apps=["hotel_reservation", "train_ticket"])


def test_exhausted_stream_names_the_slot():
    with pytest.raises(ValueError, match="slot 2"):
        plan_sequence(3, {1: 1.0}, DEFAULT_APPS, 0, iter(["a", "b"]))


def test_empty_apps_are_rejected():
    with pytest.raises(ValueError, match="apps"):
        plan_sequence(1, WEIGHTS, [], 0, _singles())


def test_zero_problems_give_an_empty_plan():
    assert plan_sequence(0, WEIGHTS, DEFAULT_APPS, 0, iter([])) == []


def test_slot_seeds_are_stable_and_distinct():
    seeds = [slot_seed(42, slot) for slot in range(1000)]
    assert seeds == [slot_seed(42, slot) for slot in range(1000)]
    assert len(set(seeds)) == len(seeds)
    assert all(0 <= s < 2**32 for s in seeds)


def test_infer_app_reads_variant_values_and_plan_ids():
    assert infer_app("incorrect_image__v_hotel_reservation_frontend_bad") == "hotel_reservation"
    assert infer_app("mf__train_ticket__n3__s9__v_") == "train_ticket"
    assert infer_app("mf__train_ticket__n3__s9__k2__v_") == "train_ticket"
    assert infer_app("target_port") is None
    assert infer_app("x__v_hotel_reservation_astronomy_shop") is None


def test_app_filter_keeps_matching_and_undeterminable_ids():
    assert filter_pool_by_apps(POOL, ["hotel_reservation", "train_ticket"]) == [
        "target_port",
        "incorrect_image__v_hotel_reservation_frontend_bad",
        "readiness_probe__v_train_ticket_3",
    ]


def test_list_stream_repeats_the_pool_in_order():
    assert list(itertools.islice(single_fault_stream(POOL, "list", 0), 7)) == POOL + POOL[:2]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_every_strategy_is_deterministic_and_stays_in_the_pool(strategy):
    first = list(itertools.islice(single_fault_stream(POOL, strategy, 11), 23))
    again = list(itertools.islice(single_fault_stream(POOL, strategy, 11), 23))
    assert first == again
    assert set(first) <= set(POOL)


@pytest.mark.parametrize("strategy", ["list", "shuffle", "grouped"])
def test_epoch_strategies_emit_each_id_once_per_pool_length(strategy):
    window = list(itertools.islice(single_fault_stream(POOL, strategy, 3), 3 * len(POOL)))
    for epoch in range(3):
        assert sorted(window[epoch * len(POOL) : (epoch + 1) * len(POOL)]) == sorted(POOL)


def test_round_robin_stream_takes_one_id_per_class_per_round():
    classes = {pid.split("__v_")[0] for pid in POOL}
    stream = list(itertools.islice(single_fault_stream(POOL, "round_robin", 3), 4 * len(classes)))
    for start in range(0, len(stream), len(classes)):
        assert {pid.split("__v_")[0] for pid in stream[start : start + len(classes)]} == classes


def test_adaptive_stream_drains_classes_in_sorted_order():
    head = list(itertools.islice(single_fault_stream(POOL, "adaptive", 0), 6))
    bases = [pid.split("__v_")[0] for pid in head]
    # Budget is the largest class (2): incorrect_image x2, readiness_probe x2, target_port x2.
    assert bases == ["incorrect_image"] * 2 + ["readiness_probe"] * 2 + ["target_port"] * 2


def test_unknown_strategy_and_empty_pool_are_rejected():
    with pytest.raises(ValueError, match="bogus"):
        single_fault_stream(POOL, "bogus", 0)
    with pytest.raises(ValueError, match="empty"):
        single_fault_stream([], "shuffle", 0)
