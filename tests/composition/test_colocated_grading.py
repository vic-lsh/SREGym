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
"""Pure grading rules of the co-located composite: probe log, collateral walk, aggregation."""

import itertools
import json
import shlex
from pathlib import Path

import pytest

from sregym.conductor.problems.composition import grading
from sregym.conductor.problems.composition.catalog import _kit  # APP_NAMESPACES: no public accessor


def _log(ok, fail, done=True):
    lines = ["PROBE_OK /"] * ok + ["PROBE_FAIL /api/products"] * fail
    return "\n".join(lines + (["PROBE_DONE"] if done else []))


@pytest.mark.parametrize(
    "ok, fail, expected",
    [(10, 0, True), (9, 1, True), (8, 2, False), (0, 10, False), (0, 0, False)],
)
def test_probe_threshold(ok, fail, expected):
    assert grading.parse_probe_log(_log(ok, fail)).ok is expected


def test_incomplete_probe_fails_even_if_every_request_passed():
    result = grading.parse_probe_log(_log(10, 0, done=False))

    assert not result.ok and not result.complete
    assert "incomplete" in result.detail()


def test_probe_log_ignores_noise_and_lists_failing_paths_once():
    log = "wget: server returned error: HTTP/1.1 500\n" + _log(18, 2)

    result = grading.parse_probe_log(log)

    assert (result.ok_count, result.total, result.failed_paths) == (18, 20, ("/api/products",))


_METADATA = Path(__file__).resolve().parents[2] / "sregym" / "service" / "metadata"


@pytest.mark.parametrize("app", sorted(_kit.APP_NAMESPACES))
def test_every_catalog_app_has_entry_points_under_its_application_name(app):
    # Regression: astronomy_shop's Application.name is "OpenTelemetry Demo Astronomy Shop"; a table
    # keyed "Astronomy Shop" failed the traffic probe on a healthy app (live control, #150).
    name = json.loads((_METADATA / f"{_kit.APP_NAMESPACES[app]}.json").read_text())["Name"]

    assert name in grading.ENTRY_POINTS


@pytest.mark.parametrize("app", sorted(grading.ENTRY_POINTS))
def test_every_entry_point_is_requested_each_round_and_quoted(app):
    entry = grading.ENTRY_POINTS[app]
    script = grading.probe_script("ns-1", entry, rounds=2)

    assert script.count("wget ") == len(entry.paths)
    for path in entry.paths:
        assert shlex.quote(f"http://{entry.service}.ns-1.svc.cluster.local:{entry.port}{path}") in script
    assert grading.probe_deadline_s(entry, 2) >= 2 * len(entry.paths) * grading.PROBE_REQUEST_TIMEOUT_S


def _pod(name, phase="Running", ready=True, waiting=None, terminated=None, labels=None):
    state = (
        {"waiting": {"reason": waiting}} if waiting else {"terminated": {"reason": terminated}} if terminated else {}
    )
    return {
        "metadata": {"name": name, "labels": labels or {}},
        "status": {"phase": phase, "containerStatuses": [{"name": name, "ready": ready, "state": state}]},
    }


@pytest.mark.parametrize(
    "pods, ok",
    [
        ([_pod("a")], True),
        ([_pod("job", phase="Succeeded", ready=False, terminated="Completed")], True),
        ([], False),
        ([_pod("a"), _pod("b", phase="Pending")], False),
        ([_pod("a", waiting="CrashLoopBackOff", ready=False)], False),
        ([_pod("a", terminated="OOMKilled")], False),
        ([_pod("a", ready=False)], False),
        ([_pod("a"), _pod("probe", phase="Failed", labels=dict(grading.PROBE_POD_LABEL))], True),
    ],
)
def test_collateral_verdict(pods, ok):
    assert grading.collateral_verdict(pods)[0] is ok


@pytest.mark.parametrize("verdicts", [list(v) for n in range(0, 4) for v in itertools.product([True, False], repeat=n)])
@pytest.mark.parametrize("collateral_ok", [True, False])
@pytest.mark.parametrize("traffic_ok", [True, False])
def test_mitigation_success_is_the_conjunction(verdicts, collateral_ok, traffic_ok):
    expected = bool(verdicts) and all(verdicts) and collateral_ok and traffic_ok

    assert grading.mitigation_success(verdicts, collateral_ok, traffic_ok) is expected
