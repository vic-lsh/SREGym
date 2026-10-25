"""The benchmark observer has bounded trace retention and memory."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def all_in_one(document):
    return document["spec"]["template"]["spec"]["containers"][0]


def test_observer_manifest_bounds_trace_retention_and_container_memory():
    docs = list(yaml.safe_load_all((ROOT / "sregym/observer/jaeger/jaeger.yaml").read_text()))
    container = all_in_one(next(doc for doc in docs if doc["kind"] == "Deployment"))
    assert "--memory.max-traces=10000" in container.get("args", [])
    assert container["resources"]["limits"]["memory"] == "2Gi"
