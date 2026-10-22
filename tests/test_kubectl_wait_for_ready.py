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

"""``KubeCtl.wait_for_ready`` fails fast on pods that cannot pull their images.

Regression: Docker Hub rate limits (HTTP 429) left pods in ImagePullBackOff and
each deploy waited the full WAIT_FOR_POD_READY_TIMEOUT (600 s) before failing.
"""

from types import SimpleNamespace

import pytest

kubectl_mod = pytest.importorskip("sregym.service.kubectl")

NS = "astronomy-shop"
RATE_LIMITED = "429 Too Many Requests - Server message: toomanyrequests: You have reached your pull rate limit"


def _container(image, ready=False, waiting=None, message=None):
    state = SimpleNamespace(waiting=SimpleNamespace(reason=waiting, message=message) if waiting else None)
    return SimpleNamespace(image=image, ready=ready, state=state)


def _pod(name, *containers, init=(), phase="Pending"):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, owner_references=None),
        status=SimpleNamespace(phase=phase, container_statuses=list(containers), init_container_statuses=list(init)),
    )


def _ready(name):
    return _pod(name, _container(f"ghcr.io/x/{name}:1", ready=True), phase="Running")


class FakeCoreV1:
    """The pod listing wait_for_ready polls: one pod list per poll, the last repeated."""

    def __init__(self, polls):
        self.polls = list(polls)
        self.calls = 0

    def list_namespaced_pod(self, namespace, label_selector=None):
        assert namespace == NS
        pods = self.polls[min(self.calls, len(self.polls) - 1)]
        self.calls += 1
        return SimpleNamespace(items=pods)


class FakeAppsV1:
    def delete_namespaced_daemon_set(self, name, namespace):
        return None


class FakeClock:
    """Records the sleeps instead of sleeping."""

    def __init__(self):
        self.slept = 0

    def __call__(self, seconds):
        self.slept += seconds


def _kubectl(polls):
    # KubeCtl() loads a kubeconfig; build one without __init__ and give it the
    # fake API objects and clock its constructor and class would have supplied.
    k = kubectl_mod.KubeCtl.__new__(kubectl_mod.KubeCtl)
    k.core_v1_api = FakeCoreV1(polls)
    k.apps_v1_api = FakeAppsV1()
    k._sleep = FakeClock()
    return k


def test_a_pod_stuck_pulling_fails_after_the_window_naming_pod_and_image() -> None:
    stuck = _pod(
        "frontend-7d9", _container("docker.io/library/nginx:1.27", waiting="ImagePullBackOff", message=RATE_LIMITED)
    )
    k = _kubectl([[_ready("cart-1"), stuck]])
    with pytest.raises(kubectl_mod.ImagePullStuckError) as err:
        k.wait_for_ready(NS, sleep=2, max_wait=600, image_pull_fail_after=60)
    assert 60 <= k._sleep.slept < 70
    assert "frontend-7d9" in str(err.value) and "docker.io/library/nginx:1.27" in str(err.value)
    assert "429" in str(err.value) and "cart-1" not in str(err.value)


def test_the_pull_state_flapping_between_reasons_still_counts_as_stuck() -> None:
    def poll(reason):
        return [_pod("mongo-0", init=[_container("docker.io/mongo:7", waiting=reason, message="pull failed")])]

    k = _kubectl([poll("ErrImagePull"), poll("ImagePullBackOff")] * 40)
    with pytest.raises(kubectl_mod.ImagePullStuckError, match="mongo-0: docker.io/mongo:7"):
        k.wait_for_ready(NS, sleep=2, max_wait=600, image_pull_fail_after=60)
    assert k._sleep.slept < 70


def test_a_pull_that_recovers_inside_the_window_keeps_waiting_until_ready() -> None:
    pulling = _pod("geo-1", _container("docker.io/geo:1", waiting="ErrImagePull", message=RATE_LIMITED))
    creating = _pod("geo-1", _container("docker.io/geo:1", waiting="ContainerCreating"))
    polls = [[pulling]] * 20 + [[creating]] * 5 + [[pulling]] * 20 + [[_ready("geo-1")]]
    k = _kubectl(polls)
    k.wait_for_ready(NS, sleep=2, max_wait=600, image_pull_fail_after=60)
    assert k._sleep.slept == 2 * (len(polls) - 1)  # never stuck 60 s in a row


def test_pods_slow_for_other_reasons_still_wait_for_the_full_timeout() -> None:
    k = _kubectl([[_pod("cart-1", _container("docker.io/cart:1", waiting="ContainerCreating"))]])
    with pytest.raises(Exception, match="Timeout") as err:
        k.wait_for_ready(NS, sleep=2, max_wait=100, image_pull_fail_after=60)
    assert not isinstance(err.value, kubectl_mod.ImagePullStuckError)
    assert k._sleep.slept == 100
