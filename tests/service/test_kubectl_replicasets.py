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

"""``KubeCtl.get_matching_replicasets`` returns the Deployment's CURRENT ReplicaSet first.

Regression: NamespaceMemoryLimit deletes ``get_matching_replicasets(...)[0]`` to
force pod recreation under its quota. After any earlier rollout the Deployment
keeps old, zero-replica ReplicaSets and the API lists them in name order, so
``[0]`` could be a stale one: deleting it is a no-op and the fault never fires.
"""

import datetime
import random
from types import SimpleNamespace

import pytest

kubectl_mod = pytest.importorskip("sregym.service.kubectl")
from kubernetes.client.rest import ApiException  # noqa: E402

DEPLOYMENT = "search"
NS = "hotel-reservation"
_T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


def _meta(name, revision=None, owner=DEPLOYMENT, created_min=0):
    annotations = {"deployment.kubernetes.io/revision": str(revision)} if revision is not None else None
    owners = [SimpleNamespace(kind="Deployment", name=owner)] if owner else None
    return SimpleNamespace(
        name=name,
        annotations=annotations,
        owner_references=owners,
        creation_timestamp=_T0 + datetime.timedelta(minutes=created_min),
    )


def _rs(name, revision, replicas, created_min, owner=DEPLOYMENT):
    return SimpleNamespace(metadata=_meta(name, revision, owner, created_min), spec=SimpleNamespace(replicas=replicas))


class FakeAppsV1:
    """The two AppsV1Api calls get_matching_replicasets makes, over fixed objects."""

    def __init__(self, replicasets, deployment_revision):
        self.replicasets = replicasets
        self.deployment_revision = deployment_revision

    def list_namespaced_replica_set(self, namespace):
        assert namespace == NS
        return SimpleNamespace(items=list(self.replicasets))

    def read_namespaced_deployment(self, name, namespace):
        if self.deployment_revision is None:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(metadata=_meta(name, self.deployment_revision, owner=None))


def _matching(replicasets, deployment_revision):
    # KubeCtl() loads a kubeconfig; build one without __init__ and give it the
    # fake API object its constructor would have created.
    k = kubectl_mod.KubeCtl.__new__(kubectl_mod.KubeCtl)
    k.apps_v1_api = FakeAppsV1(replicasets, deployment_revision)
    return [rs.metadata.name for rs in k.get_matching_replicasets(NS, DEPLOYMENT)]


# Name order puts the stale ReplicaSets first, as the API's listing does.
_HISTORY = [
    _rs("search-5c9d", revision=1, replicas=0, created_min=0),
    _rs("search-6f7a", revision=2, replicas=0, created_min=10),
    _rs("search-7b21", revision=3, replicas=1, created_min=20),
    _rs("other-1111", revision=9, replicas=1, created_min=30, owner="geo"),
]


def test_current_replicaset_is_first_after_rollouts():
    assert _matching(_HISTORY, deployment_revision=3) == ["search-7b21", "search-6f7a", "search-5c9d"]


def test_deployment_revision_wins_over_recency():
    # mid-rollback: the Deployment points at revision 2 while 3 still has pods
    names = _matching(_HISTORY, deployment_revision=2)
    assert names[0] == "search-6f7a"


def test_unreadable_deployment_falls_back_to_live_newest():
    assert _matching(_HISTORY, deployment_revision=None)[0] == "search-7b21"


def test_order_is_independent_of_listing_order():
    rng = random.Random(0)
    for _ in range(20):
        shuffled = list(_HISTORY)
        rng.shuffle(shuffled)
        assert _matching(shuffled, deployment_revision=3)[0] == "search-7b21"


def test_missing_annotations_fall_back_to_replicas_then_creation_time():
    rss = [_rs("a", None, 0, 30), _rs("b", None, 2, 5), _rs("c", None, 2, 10)]
    ordered = kubectl_mod.order_replicasets_current_first(rss, None)
    assert [rs.metadata.name for rs in ordered] == ["c", "b", "a"]
