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

"""The composite's traffic-probe pod is never reported by a safety invariant.

``CompositeMitigationOracle`` runs a busybox probe pod in the app namespace during
mitigation evaluation, just before ``Conductor._apply_safety_veto`` calls
``monitor.sample_now()``. The pod is created by the harness with the real kubeconfig, so
it never passes the agent proxy (``agent_actions``); this file covers the other two
channels, the poll tier (``check``) and the watch tier (``evaluate_object``).

Differential: every registered invariant is armed on the same namespace, then checked
with and without the probe pod present. The probe pod must change no verdict.
"""

import json
import sys
import types
from pathlib import Path

import pytest

try:  # normal path (deps installed)
    import sregym.conductor  # noqa: F401
except Exception:  # dep-light fallback: stub parents so the stdlib-only pkgs import
    _base = Path(__file__).resolve().parents[2]
    for _name, _sub in [
        ("sregym", "sregym"),
        ("sregym.conductor", "sregym/conductor"),
        ("sregym.conductor.problems", "sregym/conductor/problems"),
    ]:
        if _name not in sys.modules or not hasattr(sys.modules[_name], "__path__"):
            _m = types.ModuleType(_name)
            _m.__path__ = [str(_base / _sub)]
            sys.modules[_name] = _m

from sregym.conductor.problems.composition import grading  # noqa: E402
from sregym.conductor.safety.checks import ALL_INVARIANT_CLASSES  # noqa: E402
from sregym.conductor.safety.invariants import InvariantContext  # noqa: E402

NS = "hotel-reservation-test"


class HotelReservation:  # named like the real app class: checks scope on the class name
    name = "Hotel Reservation"
    namespace = NS


class K8STargetPortMisconfig:
    namespace = NS
    faulty_service = "user"


class ColocatedComposite:
    def __init__(self):
        self.problems = [K8STargetPortMisconfig()]
        self.app = HotelReservation()
        self.app_name = "hotel_reservation"
        self.namespace = NS
        self.safety_report_only = True


class FakeKubectl:
    """``kubectl get <resource> ... -o json`` over an in-memory namespace; anything else is NotFound."""

    def __init__(self, objects):
        self.objects = objects

    def exec_command(self, cmd, input_data=None):
        parts = cmd.split()
        if len(parts) > 2 and parts[1] == "get" and parts[2] in self.objects and "-o" in parts:
            return json.dumps({"items": self.objects[parts[2]]})
        return "Error from server (NotFound)"


def _deployment(name, image="yinfangchen/hotelreservation:latest"):
    return {
        "metadata": {"name": name, "namespace": NS},
        "spec": {
            "replicas": 1,
            "template": {"spec": {"containers": [{"name": name, "image": image}]}},
        },
        "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1},
    }


def _app_pod(name):
    return {
        "metadata": {"name": f"{name}-abc", "namespace": NS, "ownerReferences": [{"kind": "ReplicaSet"}]},
        "spec": {"containers": [{"name": name, "image": "yinfangchen/hotelreservation:latest"}]},
        "status": {"phase": "Running"},
    }


def _probe_pod():
    pod = grading.probe_pod_manifest("composite-traffic-probe-1a2b", NS, grading.ENTRY_POINTS["Hotel Reservation"])
    pod["status"] = {"phase": "Running", "containerStatuses": [{"name": "probe", "ready": True}]}
    return pod


def _world(with_probe):
    pods = [_app_pod("frontend")] + ([_probe_pod()] if with_probe else [])
    return {"deployments": [_deployment("frontend")], "pods": pods, "services": [], "networkpolicies": []}


def _verdict(violation):
    return None if violation is None else (violation.invariant, violation.detail)


@pytest.mark.parametrize("cls", ALL_INVARIANT_CLASSES, ids=lambda c: c.__name__)
def test_probe_pod_changes_no_poll_verdict(cls):
    kubectl = FakeKubectl(_world(with_probe=False))
    ctx = InvariantContext(ColocatedComposite(), kubectl, NS)
    inv = cls()
    inv.arm(ctx)
    without = _verdict(inv.check(ctx))

    kubectl.objects = _world(with_probe=True)

    assert _verdict(inv.check(ctx)) == without


@pytest.mark.parametrize("cls", ALL_INVARIANT_CLASSES, ids=lambda c: c.__name__)
def test_probe_pod_watch_event_is_not_a_violation(cls):
    kubectl = FakeKubectl(_world(with_probe=False))
    ctx = InvariantContext(ColocatedComposite(), kubectl, NS)
    inv = cls()
    inv.arm(ctx)
    kinds = {spec.kind for spec in inv.watch_specs(ctx)} if hasattr(inv, "watch_specs") else set()
    if "pods" not in kinds:
        pytest.skip(f"{cls.__name__} does not watch pods")

    for event in ("ADDED", "MODIFIED"):
        assert inv.evaluate_object(ctx, "pods", event, _probe_pod()) in (None, NotImplemented)
