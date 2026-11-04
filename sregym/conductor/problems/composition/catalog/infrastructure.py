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
"""Catalog family: pod-level infrastructure faults injected from the node or a client pod
(frozen process, netem on a pod's interface, ...).

Targets come from the app binding's workload rows (``workload_targets``): stateless services, caches
and stores alike, each with its own restart expectation (``holds_primary_data``). Apps are opt-in per
key. The state lives in the pod, so every draw declares the whole workload (field None). These keys are
catalog generation 2 (tests/composition/test_plan_generations.py).
"""

from __future__ import annotations

import random

from sregym.conductor.problems.composition.app_bindings import AppBinding, workload_targets
from sregym.conductor.problems.composition.catalog._component import Target, component_entry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "process_frozen": ("social_network",),
    "pod_network_impairment": ("social_network",),
    "connection_exhaustion": ("social_network",),
    "traffic_surge": ("social_network",),
}

# Calibrated live on the 1-worker kind lane (docs/incident-faults.md): connections held by the hog, request
# loops of the surge.
HOG_CONNECTIONS = (12000, 16000)
# Only targets whose exhaustion fails the entry point HARD in every trial (live, 3 runs each): compose-post-service
# (every compose goes through it). text-service and media-service gave intermittent or no symptom (the entry point's
# composes tolerate or retry around them), so a verdict there would be load-dependent: not drawn.
HOG_TARGETS = ("compose-post-service",)
SURGE_WORKERS = (100, 150)


# (mode, severities) per mode: delay in ms, loss in percent. Calibrated live so the symptom shows at the entry points.
NETWORK_SEVERITIES: dict[str, tuple[int, ...]] = {"delay": (3000, 6000), "loss": (90, 95)}


GENERATION = 2


def _targets(binding: AppBinding, *, protocols: tuple[str, ...], proxy: bool | None = False):
    return [
        t for t in workload_targets(binding) if t.protocol in protocols and (proxy is None or t.proxy == proxy)
    ]


def _choose_frozen(binding: AppBinding, rng: random.Random) -> Target:
    t = rng.choice(_targets(binding, protocols=("redis", "mongo", "thrift", "grpc")))
    return Target({"target": t.workload})


def _choose_netem(binding: AppBinding, rng: random.Random) -> Target:
    t = rng.choice(_targets(binding, protocols=("redis", "mongo", "thrift", "grpc")))
    mode = rng.choice(sorted(NETWORK_SEVERITIES))
    severity = rng.choice(NETWORK_SEVERITIES[mode])
    return Target({"target": t.workload, "mode": mode, "severity": severity})


def _choose_hog(binding: AppBinding, rng: random.Random) -> Target:
    t = rng.choice([t for t in _targets(binding, protocols=("thrift", "grpc")) if t.workload in HOG_TARGETS])
    return Target({"target": t.workload, "connections": rng.choice(HOG_CONNECTIONS)})


def _choose_surge(binding: AppBinding, rng: random.Random) -> Target:
    (proxy,) = _targets(binding, protocols=("http",), proxy=True)
    return Target({"target": proxy.workload, "workers": rng.choice(SURGE_WORKERS)})


# freeze and netem: a restart replaces the pod and clears the fault, so a repair may restart the target.
ENTRIES = (
    component_entry(
        "process_frozen",
        ENABLED_APPS["process_frozen"],
        "sregym.conductor.problems.process_frozen:ProcessFrozen",
        _choose_frozen,
        restarts=True,
        generation=GENERATION,
    ),
    component_entry(
        "pod_network_impairment",
        ENABLED_APPS["pod_network_impairment"],
        "sregym.conductor.problems.pod_network_impairment:PodNetworkImpairment",
        _choose_netem,
        restarts=True,
        generation=GENERATION,
    ),
    component_entry(
        "connection_exhaustion",
        ENABLED_APPS["connection_exhaustion"],
        "sregym.conductor.problems.connection_exhaustion:ConnectionExhaustion",
        _choose_hog,
        generation=GENERATION,
    ),
    component_entry(
        "traffic_surge",
        ENABLED_APPS["traffic_surge"],
        "sregym.conductor.problems.traffic_surge:TrafficSurge",
        _choose_surge,
        generation=GENERATION,
    ),
)
