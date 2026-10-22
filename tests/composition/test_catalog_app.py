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
"""Pure properties of the app-level catalog families (app_level, otel_flags, admission)."""

import itertools
import json
import random

import pytest

from sregym.conductor.problems.composition.catalog import admission, app_level, otel_flags
from sregym.conductor.problems.composition.spec import Tier, conflicts

APPS = frozenset({"hotel_reservation", "social_network", "astronomy_shop", "train_ticket"})
ENTRIES = app_level.ENTRIES + otel_flags.ENTRIES + admission.ENTRIES
BY_KEY = {e.key: e for e in ENTRIES}
SEEDS = range(64)

CASES = [(e, app) for e in ENTRIES for app in sorted(e.apps)]
CASE_IDS = [f"{e.key}-{app}" for e, app in CASES]


def draws(key, app, seeds=SEEDS):
    return [BY_KEY[key].sample(app, random.Random(s)) for s in seeds]


def test_keys_unique_across_modules():
    keys = [e.key for e in ENTRIES]
    assert len(keys) == len(set(keys))


def test_every_entry_targets_known_apps():
    for e in ENTRIES:
        assert e.apps, e.key
        assert e.apps <= APPS, e.key


@pytest.mark.parametrize(("entry", "app"), CASES, ids=CASE_IDS)
def test_sample_is_deterministic_and_well_formed(entry, app):
    for seed in SEEDS:
        draw = entry.sample(app, random.Random(seed))
        assert draw == entry.sample(app, random.Random(seed))
        assert draw.key == entry.key
        assert draw.app == app
        assert draw.footprints
        assert list(draw.params) == sorted(draw.params)
        json.dumps(draw.to_json())


@pytest.mark.parametrize("entry", ENTRIES, ids=[e.key for e in ENTRIES])
def test_sample_rejects_unsupported_app(entry):
    for app in APPS - entry.apps:
        with pytest.raises(ValueError):
            entry.sample(app, random.Random(0))


def test_a_draw_conflicts_with_itself():
    # Drawing the same fault instance twice must never be planned.
    for entry, app in CASES:
        draw = entry.sample(app, random.Random(0))
        assert conflicts(draw, draw), entry.key


@pytest.mark.parametrize(
    ("key", "param", "only"),
    [
        ("wrong_bin_usage", "faulty_service", "profile"),
        ("configmap_drift", "faulty_service", "geo"),
        ("env_variable_shadowing", "faulty_service", "frontend-proxy"),
    ],
)
def test_single_target_classes_only_sample_the_working_target(key, param, only):
    (app,) = BY_KEY[key].apps
    assert {d.param_dict()[param] for d in draws(key, app)} == {only}


# --- flag footprints ----------------------------------------------------------------------------------

FLAG_KEYS = [
    e.key
    for e in otel_flags.ENTRIES
    if any(
        fp.kind == "ConfigMap" and fp.name == "flagd-config"
        for fp in e.sample(sorted(e.apps)[0], random.Random(0)).footprints
    )
]


def test_flag_entries_cover_every_flag_class():
    assert len(FLAG_KEYS) == 13  # 11 OTel flags + TrainTicket F17/F22


def test_same_flag_draws_conflict():
    for key in FLAG_KEYS:
        (app,) = BY_KEY[key].apps
        a, b = draws(key, app, seeds=(1, 2))
        assert conflicts(a, b), key


def test_different_flag_draws_do_not_conflict():
    for ka, kb in itertools.combinations(FLAG_KEYS, 2):
        if BY_KEY[ka].apps != BY_KEY[kb].apps:
            continue
        (app,) = BY_KEY[ka].apps
        a = BY_KEY[ka].sample(app, random.Random(0))
        b = BY_KEY[kb].sample(app, random.Random(0))
        assert not conflicts(a, b), (ka, kb)


def test_flag_ids_are_deduplicated():
    flags = [
        fp.field
        for key in FLAG_KEYS
        for fp in draws(key, sorted(BY_KEY[key].apps)[0], seeds=(0,))[0].footprints
        if fp.kind == "ConfigMap"
    ]
    assert len(flags) == len(set(flags))


def test_otel_faults_inject_first():
    for e in otel_flags.ENTRIES:
        if e.apps == {"astronomy_shop"}:
            assert e.sample("astronomy_shop", random.Random(0)).tier == Tier.RESTARTS_PODS, e.key


def test_kafka_queue_problems_conflicts_with_poison_pill():
    a = BY_KEY["kafka_queue_problems"].sample("astronomy_shop", random.Random(0))
    b = BY_KEY["kafka_poison_pill_hol_block"].sample("astronomy_shop", random.Random(0))
    assert conflicts(a, b)


# --- admission ----------------------------------------------------------------------------------------

ADMISSION_KEYS = [e.key for e in admission.ENTRIES]


def all_pairs(ka, kb, app):
    return [(a, b) for a in draws(ka, app, seeds=range(16)) for b in draws(kb, app, seeds=range(16))]


@pytest.mark.parametrize(
    ("ka", "kb"),
    [
        ("admission_webhook_outage", "admission_webhook_tls_mismatch"),
        ("admission_webhook_tls_mismatch", "cumulative_admission_webhook_timeout_hotel_reservation"),
    ],
)
def test_fixed_name_pairs_always_conflict_by_tag(ka, kb):
    for app in sorted(BY_KEY[ka].apps & BY_KEY[kb].apps):
        for a, b in all_pairs(ka, kb, app):
            assert a.exclusive_tags & b.exclusive_tags, (ka, kb, app)


def test_mutating_webhook_excludes_the_other_social_network_blockers():
    mutating = BY_KEY["mutating_webhook_resource_limits_social_network"].sample("social_network", random.Random(0))
    others = [k for k in ADMISSION_KEYS if "social_network" in BY_KEY[k].apps and k != mutating.key]
    assert others
    for key in others:
        for b in draws(key, "social_network", seeds=range(16)):
            assert conflicts(mutating, b), key


def test_unrelated_blockers_compose():
    # Outage and PSA touch different objects: the tail may hold both.
    for app in sorted(APPS):
        a = BY_KEY["admission_webhook_outage"].sample(app, random.Random(0))
        b = BY_KEY["psa_restricted_blocks_recreation"].sample(app, random.Random(0))
        assert not conflicts(a, b), app


def test_admission_blockers_inject_last():
    for entry, app in CASES:
        tier = entry.sample(app, random.Random(0)).tier
        assert (tier == Tier.BLOCKS_ADMISSION) == (entry.key in ADMISSION_KEYS), entry.key


def test_admission_webhook_classes_use_every_app():
    for key in ("admission_webhook_outage", "admission_webhook_tls_mismatch", "psa_restricted_blocks_recreation"):
        assert BY_KEY[key].apps == APPS
