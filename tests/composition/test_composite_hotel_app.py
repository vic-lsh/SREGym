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
"""A hotel mf__ composite deploys its shared app as robust-skills does, whichever fault comes first:
the failure-admin ConfigMaps are filled but mounted nowhere, so mongodb-geo/rate are never patched
to mount them at /scripts (and restarted).

Real seed-42 plans are built through the real catalog. No cluster is contacted: the kubeconfig
is an offline one (the composite's real ClusterIO is built but never called), the app sources
are an empty tree with the paths the metadata names, and the deployed app gets a recording
kubectl.
"""

import pytest
import yaml

from sregym.conductor.problems.composition import resolve
from sregym.service.apps.hotel_reservation import FailureScripts, HotelReservation

MOUNTED_FIRST = "mf__hotel_reservation__n3__s1276337490__reliable__v_"  # finalizer_deadlock_controller first
ABSENT_FIRST = "mf__hotel_reservation__n2__s1993513580__reliable__v_"  # a mongo-auth fault first


@pytest.fixture(autouse=True)
def offline_hotel_app(tmp_path, monkeypatch):
    kubeconfig = tmp_path / ".kube" / "config"  # also where a bare load_kube_config() looks
    kubeconfig.parent.mkdir()
    kubeconfig.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "fake",
                "clusters": [{"name": "fake", "cluster": {"server": "https://127.0.0.1:1"}}],
                "users": [{"name": "fake", "user": {"token": "fake"}}],
                "contexts": [{"name": "fake", "context": {"cluster": "fake", "user": "fake"}}],
            }
        )
    )
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    monkeypatch.setenv("HOME", str(tmp_path))
    sources = tmp_path / "sources"
    for path in ("helm-chart/hotelreservation", "kubernetes"):
        (sources / path).mkdir(parents=True)
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(sources))


class RecordingKubectl:
    """The KubeCtl calls HotelReservation.deploy makes, recorded."""

    def __init__(self):
        self.commands = []
        self.configmaps = {}

    def exec_command(self, command):
        self.commands.append(command)
        return "NotFound" if command.startswith("kubectl get namespace") else ""

    def create_or_update_configmap(self, name, namespace, data):
        self.configmaps[name] = data

    def apply_configs(self, namespace, path):
        self.commands.append(f"apply {path}")

    def wait_for_ready(self, namespace):
        pass


def _deploy(app):
    app.kubectl = RecordingKubectl()
    app.deploy()
    return app.kubectl


@pytest.mark.parametrize("problem_id", [MOUNTED_FIRST, ABSENT_FIRST])
def test_a_hotel_composite_fills_the_failure_configmaps_but_mounts_them_nowhere(problem_id):
    composite = resolve(problem_id)

    assert isinstance(composite.app, HotelReservation)
    assert all(p.app is composite.app for p in composite.problems)
    assert composite.app.failure_scripts is FailureScripts.UNMOUNTED

    kubectl = _deploy(composite.app)

    assert not [c for c in kubectl.commands if "kubectl patch deployment" in c]
    assert kubectl.configmaps["failure-admin-geo"] and kubectl.configmaps["failure-admin-rate"]


def test_single_hotel_problems_keep_their_own_failure_script_setting():
    mounted = _deploy(HotelReservation())
    assert [c for c in mounted.commands if "kubectl patch deployment mongodb-geo" in c]
    assert [c for c in mounted.commands if "kubectl patch deployment mongodb-rate" in c]

    absent = _deploy(HotelReservation(FailureScripts.ABSENT))
    assert "failure-admin-geo" not in absent.configmaps
    assert not [c for c in absent.commands if "kubectl patch deployment" in c]
