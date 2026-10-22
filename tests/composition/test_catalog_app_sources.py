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
"""The app-level catalog samples exactly the targets SREGym's own variant tables define.

The pure catalog cannot import `variant_specs` / `variant_utils` (they import the apps), so it mirrors
their tables. These tests parse the originals and compare what the catalog actually samples.
"""

import ast
import random
from pathlib import Path

from sregym.conductor.problems.composition.catalog import admission, app_level, otel_flags

PROBLEMS = Path(__file__).resolve().parents[2] / "sregym" / "conductor" / "problems"
BY_KEY = {e.key: e for e in app_level.ENTRIES + otel_flags.ENTRIES + admission.ENTRIES}


def module_literal(filename, name):
    tree = ast.parse((PROBLEMS / filename).read_text())
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [getattr(node, "target", None)]
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {filename}")


def sampled(key, app, param, seeds=range(400)):
    return {BY_KEY[key].sample(app, random.Random(s)).param_dict()[param] for s in seeds}


def test_per_service_entries_sample_the_representative_services():
    representative = module_literal("variant_specs.py", "_REPRESENTATIVE_SERVICES")
    for key in (
        "incorrect_image",
        "admission_webhook_outage",
        "admission_webhook_tls_mismatch",
        "psa_restricted_blocks_recreation",
        "network_policy_block",
    ):
        for app in sorted(BY_KEY[key].apps):
            assert sampled(key, app, "faulty_service") == set(representative[app]), (key, app)
    assert sampled("file_descriptor_exhaustion", "hotel_reservation", "faulty_service") == set(
        representative["hotel_reservation"]
    )


def test_ephemeral_port_range_all_deployments_covers_every_hotel_deployment():
    services = module_literal("variant_utils.py", "SERVICES_BY_APP")
    entry = BY_KEY["ephemeral_port_range_hotel_reservation"]
    names = set()
    for seed in range(64):
        draw = entry.sample("hotel_reservation", random.Random(seed))
        if draw.param_dict()["sysctl_scope"] == "all_deployments":
            names = {fp.name for fp in draw.footprints}
            break
    assert names == set(services["hotel_reservation"])


def test_astronomy_env_vars_match_variant_specs():
    env_vars = module_literal("variant_specs.py", "_ASTRONOMY_SHOP_ENV_VARS")
    seen = {
        (d["env_var"], d["env_var_value"])
        for d in (
            BY_KEY["missing_env_variable"].sample("astronomy_shop", random.Random(s)).param_dict() for s in range(400)
        )
    }
    assert seen == set(env_vars.items())
