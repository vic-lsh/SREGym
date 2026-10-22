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
"""The Kubernetes-object catalog family stays faithful to the problem classes it builds.

Pure checks: problem sources are parsed with `ast`, never imported (they need the full stack).
"""

import ast
import random
import re
from pathlib import Path

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.catalog import k8s_objects
from sregym.conductor.problems.composition.spec import Footprint, Tier, conflicts

PROBLEMS_DIR = Path(__file__).resolve().parents[2] / "sregym" / "conductor" / "problems"
ALL_ENTRIES = {e.key: e for e in catalog.entries()}


def _module_ast(module):
    return ast.parse((PROBLEMS_DIR / f"{module}.py").read_text())


def _init_params(module, cls):
    for node in ast.walk(_module_ast(module)):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    args = item.args
                    return {a.arg for a in args.args[1:] + args.kwonlyargs}
            return set()
    raise AssertionError(f"class {cls} not found in {module}.py")


def test_representative_services_match_variant_specs():
    for node in _module_ast("variant_specs").body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "_REPRESENTATIVE_SERVICES":
            expected = ast.literal_eval(node.value)
            break
    else:
        raise AssertionError("_REPRESENTATIVE_SERVICES not found in variant_specs.py")
    assert {app: list(svcs) for app, svcs in k8s_objects.REPRESENTATIVE_SERVICES.items()} == expected


@pytest.mark.parametrize("entry", k8s_objects.ENTRIES, ids=lambda e: e.key)
def test_draw_params_are_constructor_arguments(entry):
    builder = entry.build
    assert isinstance(builder, k8s_objects.ProblemBuilder)
    accepted = _init_params(builder.module, builder.cls)
    for app in sorted(entry.apps):
        for seed in range(25):
            params = entry.sample(app, random.Random(seed)).param_dict()
            assert set(params) <= accepted, f"{builder.cls} does not accept {set(params) - accepted}"
            if "app_name" in params:
                assert params["app_name"] == app


def _draw(key, app, **wanted):
    """First draw of `key` for `app` whose params include `wanted` (searching rng seeds)."""
    entry = ALL_ENTRIES[key]
    for seed in range(2000):
        draw = entry.sample(app, random.Random(seed))
        if wanted.items() <= draw.param_dict().items():
            return draw
    raise AssertionError(f"no draw of {key} with {wanted}")


def test_snapshot_restoring_fault_owns_its_deployment_but_not_the_service():
    # Recovery snapshots are per fault (inject_virtual `_backup_path`), so MissingService and a
    # probe fault on one service no longer share a backup file: they touch different objects.
    readiness = _draw("readiness_probe_misconfiguration", "hotel_reservation", faulty_service="geo")
    missing = _draw("missing_service", "hotel_reservation", faulty_service="geo")
    assert not conflicts(readiness, missing)
    liveness = _draw("liveness_probe_misconfiguration", "hotel_reservation", faulty_service="geo")
    assert conflicts(readiness, liveness)


def test_field_level_faults_on_one_service_compose():
    port = _draw("k8s_target_port_misconfig", "social_network", faulty_service="user-service")
    selector = _draw("wrong_service_selector", "social_network", faulty_service="user-service", replica_count=1)
    assert not conflicts(port, selector)


def test_two_restart_based_faults_on_one_deployment_conflict():
    rolling = _draw("rolling_update_misconfigured", "astronomy_shop", faulty_service="cart")
    dns = _draw("wrong_dns_policy", "astronomy_shop", faulty_service="cart")
    assert conflicts(rolling, dns)


def test_fixed_name_resources_admit_one_instance():
    a = _draw("service_port_conflict", "train_ticket", faulty_service="ts-auth-service")
    b = _draw("service_port_conflict", "train_ticket", faulty_service="ts-user-service")
    assert conflicts(a, b)


def test_quota_is_injected_last_and_namespace_pod_wipe_first():
    quota = _draw("namespace_memory_limit", "hotel_reservation")
    missing = _draw("missing_service", "hotel_reservation")
    assert quota.tier is Tier.BLOCKS_ADMISSION
    assert missing.tier is Tier.RESTARTS_PODS
    assert Footprint("ResourceQuota", "memory-limit-quota") in quota.footprints


_TRAIN_TICKET_DEPLOY = (
    Path(__file__).resolve().parents[2]
    / "SREGym-applications/train-ticket/deploy-job/deployment/kubernetes-manifests/quickstart-k8s/yamls/deploy.yaml"
)


def test_train_ticket_draws_name_only_deployed_services():
    # Regression (live #150): `ts-ticketinfo-service` is not in the deployed manifests, so every
    # draw naming it failed to inject. The manifests are parsed as text (no yaml dependency).
    if not _TRAIN_TICKET_DEPLOY.is_file():
        pytest.skip("SREGym-applications submodule is not checked out")
    text = _TRAIN_TICKET_DEPLOY.read_text()
    deployed = {
        m.group(1)
        for doc in text.split("\n---")
        if "\nkind: Deployment" in "\n" + doc
        for m in [re.search(r"^  name: (\S+)$", doc, re.M)]
        if m
    }
    assert "ts-auth-service" in deployed  # the parse found the manifests
    named = set()
    for entry in catalog.entries_for("train_ticket"):
        for seed in range(40):
            for value in entry.sample("train_ticket", random.Random(seed)).param_dict().values():
                if isinstance(value, str) and value.startswith("ts-"):
                    named.add(value)
    assert named and named <= deployed, sorted(named - deployed)
