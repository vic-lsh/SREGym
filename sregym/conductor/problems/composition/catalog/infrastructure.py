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
key. The state lives in the pod, so every draw declares the whole workload (field None).
"""

from __future__ import annotations

import random
from typing import Any

from sregym.conductor.problems.composition.app_bindings import BINDINGS, workload_targets
from sregym.conductor.problems.composition.catalog._kit import Pick, deployment, entry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "process_frozen": ("social_network",),
    "pod_network_impairment": ("social_network",),
    "connection_exhaustion": ("social_network",),
    "traffic_surge": ("social_network",),
}

# Calibrated live on the 1-worker kind lane (docs/incident-faults.md): connections held by the hog, request
# loops of the surge.
HOG_CONNECTIONS = (6000, 8000)
SURGE_WORKERS = (100, 150)


# (mode, severities) per mode: delay in ms, loss in percent. Calibrated live so the symptom shows at the entry points.
NETWORK_SEVERITIES: dict[str, tuple[int, ...]] = {"delay": (3000, 6000), "loss": (90, 95)}


def _targets(app: str, *, protocols: tuple[str, ...], proxy: bool | None = False):
    return [
        t for t in workload_targets(BINDINGS[app]) if t.protocol in protocols and (proxy is None or t.proxy == proxy)
    ]


def _choose_frozen(app: str, rng: random.Random) -> Pick:
    t = rng.choice(_targets(app, protocols=("redis", "mongo", "thrift", "grpc")))
    return Pick({"target": t.workload}, (deployment(t.workload),))


def _build_frozen(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.process_frozen import ProcessFrozen

    return ProcessFrozen(app_name=app, **params)


def _choose_netem(app: str, rng: random.Random) -> Pick:
    t = rng.choice(_targets(app, protocols=("redis", "mongo", "thrift", "grpc")))
    mode = rng.choice(sorted(NETWORK_SEVERITIES))
    severity = rng.choice(NETWORK_SEVERITIES[mode])
    return Pick({"target": t.workload, "mode": mode, "severity": severity}, (deployment(t.workload),))


def _build_netem(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.pod_network_impairment import PodNetworkImpairment

    return PodNetworkImpairment(app_name=app, **params)


def _choose_hog(app: str, rng: random.Random) -> Pick:
    t = rng.choice(_targets(app, protocols=("thrift", "grpc")))
    return Pick({"target": t.workload, "connections": rng.choice(HOG_CONNECTIONS)}, (deployment(t.workload),))


def _build_hog(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.connection_exhaustion import ConnectionExhaustion

    return ConnectionExhaustion(app_name=app, **params)


def _choose_surge(app: str, rng: random.Random) -> Pick:
    (proxy,) = _targets(app, protocols=("http",), proxy=True)
    return Pick({"target": proxy.workload, "workers": rng.choice(SURGE_WORKERS)}, (deployment(proxy.workload),))


def _build_surge(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.traffic_surge import TrafficSurge

    return TrafficSurge(app_name=app, **params)


ENTRIES = (
    entry("process_frozen", ENABLED_APPS["process_frozen"], _choose_frozen, _build_frozen),
    entry("pod_network_impairment", ENABLED_APPS["pod_network_impairment"], _choose_netem, _build_netem),
    entry("connection_exhaustion", ENABLED_APPS["connection_exhaustion"], _choose_hog, _build_hog),
    entry("traffic_surge", ENABLED_APPS["traffic_surge"], _choose_surge, _build_surge),
)
