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


def test_directory_rendering_bounds_hotel_without_rewriting_source(tmp_path):
    from sregym.service.jaeger_memory import bounded_manifests

    source = tmp_path / "hotel"
    source.mkdir()
    manifest = source / "jaeger.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "kind": "Deployment",
                "spec": {
                    "template": {"spec": {"containers": [{"name": "jaeger", "image": "jaegertracing/all-in-one:1.57"}]}}
                },
            }
        )
    )
    original = manifest.read_bytes()
    with bounded_manifests(source) as rendered:
        container = all_in_one(yaml.safe_load((rendered / manifest.name).read_text()))
        assert "--memory.max-traces=10000" in container["args"]
        assert container["resources"]["limits"]["memory"] == "2Gi"
    assert manifest.read_bytes() == original
    assert not rendered.exists()


def test_registry_port_does_not_hide_an_all_in_one():
    from sregym.service.jaeger_memory import bound_document

    doc = {
        "kind": "Pod",
        "spec": {"containers": [{"name": "jaeger", "image": "localhost:5000/jaegertracing/all-in-one:1.57"}]},
    }
    bound_document(doc)
    assert doc["spec"]["containers"][0]["resources"]["limits"]["memory"] == "2Gi"


def test_astronomy_v2_memory_is_already_bounded():
    values = yaml.safe_load(
        (ROOT / "SREGym-applications/astronomy-shop/charts/opentelemetry-demo/values.yaml").read_text()
    )
    jaeger = values["jaeger"]
    config = yaml.safe_load(jaeger["userconfig"])
    memory = config["extensions"]["jaeger_storage"]["backends"]["memory_backend"]["memory"]
    assert memory["max_traces"] == "${env:MEMORY_MAX_TRACES}"
    assert (
        int(next(row["value"] for row in jaeger["jaeger"]["extraEnv"] if row["name"] == "MEMORY_MAX_TRACES")) == 25000
    )
    assert jaeger["jaeger"]["resources"]["limits"]["memory"] == "600Mi"
