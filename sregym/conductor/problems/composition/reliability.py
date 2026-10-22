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
}


def reliable_entries(app: str, entries: Sequence[CatalogEntry]) -> tuple[CatalogEntry, ...]:
    """*entries* without those flagged unreliable for *app* (others' flags do not matter)."""
    return tuple(e for e in entries if (e.key, app) not in UNRELIABLE)
