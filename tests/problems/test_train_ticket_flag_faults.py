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
"""Train Ticket feature-flag faults: injection fails loudly, and the deploy leaves no finished pod."""

import pytest

import sregym.conductor.problems.train_ticket_f22 as f22_module
import sregym.conductor.problems.trainticket_f17 as f17_module
import sregym.service.apps.train_ticket as train_ticket_module
from sregym.generators.fault.inject_tt import TrainTicketFaultInjector


class _Injector:
    result = True
    calls = []

    def __init__(self, namespace):
        self.namespace = namespace

    def _inject(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


def _problem(module, cls_name):
    cls = getattr(module, cls_name)
    problem = cls.__new__(cls)
    problem.namespace = "train-ticket"
    problem.fault_injected = False
    problem._decoy_count = 3
    problem._decoy_flag_count = "low"
    return problem


@pytest.mark.parametrize(
    ("module", "cls_name", "flag"),
    [(f17_module, "TrainTicketF17", "tt-feat-17"), (f22_module, "TrainTicketF22", "tt-feat-22")],
)
def test_inject_raises_when_the_flag_was_not_turned_on(monkeypatch, module, cls_name, flag):
    monkeypatch.setattr(module, "TrainTicketFaultInjector", _Injector)
    monkeypatch.setattr(_Injector, "calls", [])
    monkeypatch.setattr(_Injector, "result", False)
    problem = _problem(module, cls_name)

    with pytest.raises(RuntimeError, match=flag):
        problem.inject_fault()

    assert problem.fault_injected is False
    assert _Injector.calls == [{"fault_type": flag, "decoy_count": 3}]


def test_inject_succeeds_when_the_flag_is_on(monkeypatch):
    monkeypatch.setattr(f17_module, "TrainTicketFaultInjector", _Injector)
    monkeypatch.setattr(_Injector, "result", True)
    problem = _problem(f17_module, "TrainTicketF17")

    problem.inject_fault()

    assert problem.fault_injected is True


@pytest.mark.parametrize(("decoy_count", "activated"), [(0, []), (3, [3])])
def test_injector_accepts_a_decoy_count(decoy_count, activated):
    """Regression: the problems pass decoy_count, which _inject did not accept (TypeError)."""
    injector = TrainTicketFaultInjector.__new__(TrainTicketFaultInjector)
    seen = []
    injector.activate_decoy_flags = lambda count: seen.append(count) or True
    injector._set_fault_state = lambda fault_type, state: (fault_type, state) == ("tt-feat-17", "on")

    assert injector._inject(fault_type="tt-feat-17", decoy_count=decoy_count) is True
    assert seen == activated


class _KubeCtl:
    def __init__(self):
        self.commands = []

    def create_namespace_if_not_exist(self, namespace):
        pass

    def wait_for_job_completion(self, **kwargs):
        self.commands.append("<wait train-ticket-deploy>")

    def exec_command(self, command):
        self.commands.append(command)
        return ""


def test_deploy_deletes_the_finished_deploy_job(monkeypatch):
    app = train_ticket_module.TrainTicket.__new__(train_ticket_module.TrainTicket)
    app.namespace = "train-ticket"
    app.helm_configs = {}
    app.kubectl = _KubeCtl()
    app._is_train_ticket_deployed = lambda: False
    app._deploy_flagd_infrastructure = lambda: None
    app._deploy_load_generator = lambda: None
    monkeypatch.setattr(train_ticket_module.Helm, "install", staticmethod(lambda **kwargs: None))

    app.deploy()

    wait = app.kubectl.commands.index("<wait train-ticket-deploy>")
    assert app.kubectl.commands[wait + 1].startswith("kubectl delete job train-ticket-deploy -n train-ticket")
