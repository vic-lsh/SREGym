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
"""Catalog entries flagged unreliable for an app, each with the reason.

Pure module: standard library only. A plan id with the ``__reliable`` suffix draws from
the catalog without these entries (see ``generator.plan_for``); every other id draws
from the whole catalog, so existing ids keep their plans. Evidence for each flag is in
docs/robust-impl/findings/sregym-composition.md.

Flags are per (catalog key, app), not per draw: excluding a whole entry keeps the
sampler's random stream simple and deterministic. So a flag may exclude some draws that
would have been fine (noted in its reason). Adding or removing a flag changes the plans
of ``__reliable`` ids; the run manifest records the commit and the flag list.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from sregym.conductor.problems.composition.spec import CatalogEntry

UNRELIABLE: Mapping[tuple[str, str], str] = {
    ("resource_request_too_small", "hotel_reservation"): (
        "No symptom: with the 10Mi memory limit the recommendation container still fits when idle, "
        "runs with 0 restarts and serves traffic, and its namespace-wide MitigationOracle passes whenever "
        "nothing else is broken, so the verdict carries no information. Measured on recommendation only; "
        "flagged for every hotel service because the symptom depends on runtime memory use."
    ),
    ("missing_service", "hotel_reservation"): (
        "No user-visible effect: hotel services find each other through consul, not Kubernetes Services, "
        "so deleting a gRPC service's Service changes nothing users see; the oracle grades the object. "
        "The frontend Service (the traffic entry) is the exception, but entries are flagged whole."
    ),
    ("wrong_service_selector", "hotel_reservation"): (
        "No user-visible effect: a Service with no endpoints is unused under consul-based discovery "
        "(missed at K=4 with traffic 18/18 in the nested experiment); the oracle grades the object. "
        "The frontend Service is the exception, but entries are flagged whole."
    ),
    ("cumulative_admission_webhook_timeout_hotel_reservation", "hotel_reservation"): (
        "Flaky injection at N=8: its single-trigger self-check failed in 3/3 runs of one 8-fault plan "
        "under load (enforcement and webhook registration lag); the three-trigger fix passed twice, but "
        "the diagnosis is not proven."
    ),
    # Generation-2 kinds that have not validated on 3 seeds with every trial VALID. Evidence: validation/
    # {process,redis,mongo3,hotel} (FIN handoff, 2026-11-04); the reason names what is missing.
    ("mongo_write_lock", "social_network"): (
        "VAL2 seeds 1 and 2 all VALID (6/6 incl. data_lost restarts); seed 5 restart decoys INVALID (social-graph-mongodb "
        "empty after reseed, no witness) and seed 7 baseline INVALID in earlier runs. 2 of 3 seeds; seed 3 not run (time box)."
    ),
    ("mongo_poison_document", "social_network"): (
        "Not validated: VAL2 seed 1 reference VALID, flush_cache and restore_source VALID, killed at the time box before "
        "the restart decoys; earlier references were INVALID on seeds 1, 4, 6 (ids not found on a lane left by the lock run). 0 of 3 seeds complete."
    ),
    (
        "redis_writes_paused",
        "social_network",
    ): "1 of 3 seeds all VALID (seed 5, user-timeline-redis); VAL2 seed 1 (home-timeline-redis) VALID through the restart decoy, killed at the time box before the rollout_restart decoy.",
    # hotel_reservation: only mongo_write_lock validated (3 seeds, 6 VALID trials each).
    ("memcached_poisoned_entries", "hotel_reservation"): (
        "Seeds 1 and 3 FAIL: a reference/acceptable repair reads data_lost (seeded /hotels read changed); seed 2 VALID."
    ),
    ("mongo_write_rejection", "hotel_reservation"): (
        "Decoy trials INVALID on all seeds (inconclusive verdicts), seed 2 reference FAIL data_lost."
    ),
    ("mongo_collection_renamed", "hotel_reservation"): (
        "Seeds 1 and 3 all VALID; seed 2 decoys FAIL (fault not visible to the traffic probe). 2 of 3 seeds."
    ),
    ("mongo_poison_document", "hotel_reservation"): (
        "Decoy trials FAIL on all 3 seeds: the poisoned documents leave no entry-point symptom (traffic 15/15)."
    ),
    ("traffic_surge", "social_network"): (
        "Reference does not reliably pass: stopping the client clears the cause, but the surge OOM-kills "
        "user-timeline-service (128Mi; its Mongo sits at its 100m CPU limit) and the pod can sit in "
        "CrashLoopBackOff for minutes, so the entry point still fails at verdict time (14/18 and 29-31/36 in "
        "three live runs) although a later health check passes. A correct repair would also restart the "
        "crashed pods; load-sensitive, so dropped pending a redesign (evidence: validation/process/fp2)."
    ),
}


# Flags that apply only to plans of catalog generation >= the given one: a generation that revises how an
# existing kind draws (the scope of single-record data faults) must not make that kind unreliable for the
# earlier generations' ids, which draw it as they always did.
# Generation-3 scope (validation/fs, FS handoff, 2026-11-05). A kind is reliable at g3 with 3 seeds where every trial is
# VALID; none reached it (2 seeds each in the 3 h box), so every revised kind is flagged from generation 3.
UNRELIABLE_FROM: Mapping[tuple[str, str], tuple[int, str]] = {
    ("redis_poisoned_timeline_entry", "social_network"): (
        3,
        "2 of 3 seeds, every trial VALID: home-timeline-redis wide (s2) and partial 0.5 (s5), 7/7 each incl. data_lost "
        "restarts; the social-graph-redis follower-set variants (compose-and-read check) were not run.",
    ),
    ("redis_timeline_key_renamed", "social_network"): (
        3,
        "2 of 3 seeds, every trial VALID: partial 0.25 (s1) and wide (s4), 8/8 each incl. data_lost restarts.",
    ),
    ("social_graph_stale_cache", "social_network"): (
        3,
        "2 of 3 seeds, every trial VALID: wide (s4) and partial 0.5 (s2), 7/7 each (readd_first_author rerun alone after "
        "a decoy-listing fix).",
    ),
    ("memcached_poisoned_entries", "social_network"): (
        3,
        "Scoped draw not validated: only prepare_target ran live (cold cache warmed by 40 seeded users' reads, 380 "
        "seeded post entries chosen); no trial.",
    ),
}


def reliable_entries(app: str, entries: Sequence[CatalogEntry], generation: int = 1) -> tuple[CatalogEntry, ...]:
    """*entries* without those flagged unreliable for *app* (others' flags do not matter). `generation` is the
    catalog generation of the plan: flags in ``UNRELIABLE_FROM`` apply from their generation on."""

    def flagged(e: CatalogEntry) -> bool:
        later = UNRELIABLE_FROM.get((e.key, app))
        return (e.key, app) in UNRELIABLE or (later is not None and generation >= later[0])

    return tuple(e for e in entries if not flagged(e))
