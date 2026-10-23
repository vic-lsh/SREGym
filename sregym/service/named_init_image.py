"""Named init-container image mutation and exact recovery checks."""

from __future__ import annotations

from typing import Any


def init_image_patch(deployment: dict[str, Any], name: str, image: str) -> dict[str, Any]:
    containers = deployment["spec"]["template"]["spec"].get("initContainers", [])
    if sum(c["name"] == name for c in containers) != 1:
        raise ValueError(f"expected exactly one init container named {name!r}")
    return {"spec": {"template": {"spec": {"initContainers": [{"name": name, "image": image}]}}}}


def init_image_recovered(deployment: dict[str, Any], name: str, image: str) -> bool:
    containers = deployment["spec"]["template"]["spec"].get("initContainers", [])
    matches = [c for c in containers if c["name"] == name]
    status = deployment.get("status", {})
    desired = deployment["spec"].get("replicas", 1)
    return bool(
        len(matches) == 1
        and matches[0]["image"] == image
        and desired > 0
        and status.get("observedGeneration", 0) >= deployment["metadata"].get("generation", 1)
        and status.get("updatedReplicas", 0) == desired
        and status.get("availableReplicas", 0) >= desired
    )
