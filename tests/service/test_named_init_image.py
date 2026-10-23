"""Named init image edits must preserve all other containers and init commands."""

import copy

import pytest

from sregym.service.named_init_image import init_image_patch, init_image_recovered


def deployment():
    return {
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "app", "image": "app:v1"}],
                    "initContainers": [
                        {"name": "other", "image": "other:v1"},
                        {"name": "wait-for-mongodb", "image": "busybox:1.36", "command": ["sh", "-c", "wait"]},
                    ],
                }
            },
        },
        "status": {"observedGeneration": 3, "updatedReplicas": 1, "availableReplicas": 1},
        "metadata": {"generation": 3},
    }


def test_patch_targets_name_only():
    original = deployment()
    saved = copy.deepcopy(original)
    assert init_image_patch(original, "wait-for-mongodb", "invalid/image:bad") == {
        "spec": {"template": {"spec": {"initContainers": [{"name": "wait-for-mongodb", "image": "invalid/image:bad"}]}}}
    }
    assert original == saved
    assert init_image_recovered(original, "wait-for-mongodb", "busybox:1.36")
    assert not init_image_recovered(original, "wait-for-mongodb", "different:image")


def test_recovery_requires_rollout_and_named_init():
    current = deployment()
    current["status"]["availableReplicas"] = 0
    assert not init_image_recovered(current, "wait-for-mongodb", "busybox:1.36")
    with pytest.raises(ValueError, match="exactly one"):
        init_image_patch(current, "missing", "bad")
