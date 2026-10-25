"""Rendered all-in-one storage must remain bounded on long-lived lanes."""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def all_in_one(document):
    return document["spec"]["template"]["spec"]["containers"][0]


def test_observer_manifest_bounds_trace_retention_and_container_memory():
    docs = list(yaml.safe_load_all((ROOT / "sregym/observer/jaeger/jaeger.yaml").read_text()))
    container = all_in_one(next(doc for doc in docs if doc["kind"] == "Deployment"))
    assert "--memory.max-traces=10000" in container.get("args", [])
    assert container["resources"]["limits"]["memory"] == "2Gi"


@pytest.mark.parametrize("kind", ["Deployment", "StatefulSet", "DaemonSet", "Job", "Pod", "CronJob"])
def test_rendered_app_local_all_in_one_is_bounded(kind):
    from sregym.service.jaeger_memory import bound_document

    container = {
        "name": "jaeger",
        "image": "jaegertracing/all-in-one:1.57",
        "args": ["--collector.otlp.enabled=true", "--memory.max-traces", "0"],
    }
    pod = {"containers": [container]}
    spec = pod if kind == "Pod" else {"template": {"spec": pod}}
    if kind == "CronJob":
        spec = {"jobTemplate": {"spec": spec}}
    doc = {"kind": kind, "spec": spec}
    bound_document(doc)
    assert container["args"] == ["--collector.otlp.enabled=true", "--memory.max-traces=10000"]
    assert container["resources"]["limits"]["memory"] == "2Gi"
    assert container["resources"]["requests"]["memory"] == "256Mi"


def test_persistent_storage_is_unchanged():
    from sregym.service.jaeger_memory import bound_document

    doc = {
        "kind": "Pod",
        "spec": {
            "containers": [
                {"image": "jaegertracing/all-in-one:1.57", "env": [{"name": "SPAN_STORAGE_TYPE", "value": "badger"}]}
            ]
        },
    }
    before = yaml.safe_dump(doc)
    bound_document(doc)
    assert yaml.safe_dump(doc) == before


def test_postrenderer_preserves_unrelated_objects_and_bounds_list_items():
    documents = [
        {"kind": "Service", "spec": {"ports": [{"port": 16686}]}},
        {
            "kind": "List",
            "items": [
                {"kind": "Pod", "spec": {"containers": [{"name": "jaeger", "image": "jaegertracing/all-in-one:1.57"}]}}
            ],
        },
    ]
    result = subprocess.run(
        [sys.executable, str(ROOT / "sregym/service/jaeger_memory.py")],
        input=yaml.safe_dump_all(documents),
        text=True,
        capture_output=True,
        check=True,
    )
    actual = list(yaml.safe_load_all(result.stdout))
    assert actual[0] == documents[0]
    assert actual[1]["items"][0]["spec"]["containers"][0]["resources"]["limits"]["memory"] == "2Gi"
