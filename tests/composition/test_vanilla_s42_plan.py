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
"""The planned run of robust-skills' vanilla Claude Code SREGym run (seed 42) plans the same here.

The fixture pins the run's 100 problem ids and, per slot, the drawn fault keys, params and tiers.
A catalog, reliability-flag or generator change that alters any of them fails here.
"""

import json
from pathlib import Path

import pytest

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.generator import parse_plan_id, parse_weights, plan_for
from sregym.conductor.problems.composition.schedule import plan_sequence
from sregym.parallel_runner import _args_for_tests, build_static_plan

RUN = json.loads((Path(__file__).parent / "fixtures" / "vanilla_s42_n100_plan.json").read_text())
RUN_IDS = [plan["id"] for plan in RUN["plans"]]


def test_schedule_plans_the_run_ids():
    ids = plan_sequence(
        RUN["num_problems"],
        parse_weights(RUN["fault_count_weights"]),
        RUN["apps"],
        RUN["seed"],
        iter(()),
        reliable=RUN["exclude_unreliable_faults"],
    )
    assert ids == RUN_IDS


@pytest.mark.parametrize("slot", range(len(RUN["plans"])))
def test_each_slot_draws_the_run_faults(slot):
    expected = RUN["plans"][slot]
    ref = parse_plan_id(expected["id"])
    plan = plan_for(ref, catalog.entries_for(ref.app))
    drawn = [{"key": d.key, "params": d.param_dict(), "tier": d.tier.name} for d in plan.draws]
    assert drawn == expected["draws"]


def test_sequence_flags_plan_the_run_ids():
    args = _args_for_tests(
        sequence_len=RUN["num_problems"],
        sequence_seed=RUN["seed"],
        fault_count_weights=RUN["fault_count_weights"],
        composition_apps=RUN["apps"],
        exclude_unreliable_faults=True,
    )
    assert [task.problem_id for task in build_static_plan(args)] == RUN_IDS


def test_without_the_flag_ids_carry_no_reliable_suffix():
    args = _args_for_tests(
        sequence_len=RUN["num_problems"],
        sequence_seed=RUN["seed"],
        fault_count_weights=RUN["fault_count_weights"],
        composition_apps=RUN["apps"],
    )
    ids = [task.problem_id for task in build_static_plan(args)]
    assert ids == [pid.replace("__reliable", "") for pid in RUN_IDS]
