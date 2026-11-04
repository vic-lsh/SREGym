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
        "Seed 1 all VALID; seed 5 restart decoys INVALID (social-graph-mongodb empty after reseed, no witness) and "
        "seed 7 baseline INVALID; the rerun (mongo_write_lock3) did not finish valid. 1 of 3 seeds. VAL rerun (seed 1): "
        "reference, restore_source and rollout_restart VALID, killed at the time box before the restart decoys."
    ),
    ("mongo_poison_document", "social_network"): (
        "Not validated: the reference is INVALID on seeds 1, 4, 6 (the poisoned post ids were not found in "
        "post-storage-mongodb on the lane left by the lock run), so no decoy trial ran. 0 of 3 seeds."
    ),
    ("redis_acl_revoked", "social_network"): (
        "Validated except restart decoys: home-timeline-redis read and write modes, seeds 1-3 all VALID "
        "(reference, restore_source, consumer rollout) and user-timeline-redis seed 7 all VALID incl. restarts; "
        "the restart decoys on home-timeline-redis were skipped (reseeding 962x10 takes ~20 min)."
    ),
    ("redis_out_of_memory", "social_network"): (
        "2 of 3 seeds all VALID (seed 9 user-timeline-redis; VAL seed 1 home-timeline-redis incl. data_lost restarts); "
        "seeds 2, 3 not finished (time box, host load 100+: one trial took 5-10 min)."
    ),
    ("redis_writes_paused", "social_network"): "1 of 3 seeds run (seed 5, user-timeline-redis, all 6 trials VALID).",
    ("redis_poisoned_timeline_entry", "social_network"): (
        "Seeds 7 and 1 all VALID; seed 5 (social-graph-redis) FAIL on its decoy trials, not diagnosed. 2 of 3 seeds."
    ),
    ("redis_timeline_key_renamed", "social_network"): (
        "1 of 3 seeds run (seed 1, all 8 VALID, incl. data-losing decoys); the other seeds hit the same target."
    ),
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


def reliable_entries(app: str, entries: Sequence[CatalogEntry]) -> tuple[CatalogEntry, ...]:
    """*entries* without those flagged unreliable for *app* (others' flags do not matter)."""
    return tuple(e for e in entries if (e.key, app) not in UNRELIABLE)
