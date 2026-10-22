from types import SimpleNamespace

from sregym.generators.fault.inject_kafka import (
    ARCHIVER_SCRIPT,
    CONSUMER_SCRIPT,
    PRODUCER_SCRIPT,
    KafkaFaultInjector,
)


def _injector():
    injector = object.__new__(KafkaFaultInjector)
    injector.namespace = "astronomy-shop"
    injector.poison_offset = KafkaFaultInjector.SEED_RECORD_COUNT
    return injector


def test_injection_seeds_the_poison_record_after_the_seed_records_and_waits_for_the_stall():
    events = []
    injector = _injector()
    injector._apply_configmap = lambda: events.append(("configmap",))
    injector._apply_pipeline_deployment = lambda name, script, extra_env: events.append(
        ("deploy", name, script, {e["name"]: e["value"] for e in extra_env})
    )
    injector._wait_deployment_ready = lambda name: events.append(("ready", name))
    injector._wait_for_log = lambda deployment, substring, timeout: events.append(("log", deployment, substring))

    assert injector.inject() == 20

    assert events == [
        ("configmap",),
        (
            "deploy",
            "order-stream",
            "producer.py",
            {"SEED_RECORD_COUNT": "20", "POISON_RECORD": "CORRUPTED-RECORD-NOT-VALID-JSON-POISON-PILL"},
        ),
        ("ready", "order-stream"),
        ("log", "order-stream", "SEED COMPLETE"),
        ("deploy", "orders-archiver", "archiver.py", {}),
        ("ready", "orders-archiver"),
        ("deploy", "orders-validator", "consumer.py", {}),
        ("ready", "orders-validator"),
        ("log", "orders-validator", "head-of-line block"),
    ]


def test_recovery_restarts_the_consumer_in_lenient_mode_without_a_rolling_update():
    events = []
    injector = _injector()
    injector._scale_consumer = lambda replicas: events.append(("scale", replicas))
    injector._wait_consumer_pods_gone = lambda: events.append(("gone",))
    injector._patch_consumer_lenient = lambda: events.append(("lenient",))
    injector._wait_deployment_ready = lambda name: events.append(("ready", name))

    injector.recover()

    assert events == [("scale", 0), ("gone",), ("lenient",), ("scale", 1), ("ready", "orders-validator")]


def test_lenient_patch_sets_only_the_consumer_env():
    patches = []
    injector = _injector()
    injector.kubectl = SimpleNamespace(
        apps_v1_api=SimpleNamespace(
            patch_namespaced_deployment=lambda name, namespace, body: patches.append((name, namespace, body))
        )
    )

    injector._patch_consumer_lenient()

    ((name, namespace, body),) = patches
    assert (name, namespace) == ("orders-validator", "astronomy-shop")
    assert body["spec"]["template"]["spec"]["containers"] == [
        {"name": "orders-validator", "env": [{"name": "LENIENT", "value": "true"}]}
    ]


def test_scripts_log_the_markers_injection_and_grading_wait_for():
    assert "SEED COMPLETE" in PRODUCER_SCRIPT
    assert "head-of-line block" in CONSUMER_SCRIPT
    assert 'out.produce(OUTPUT_TOPIC, value=str(msg.offset()).encode("utf-8"))' in CONSUMER_SCRIPT
    assert "AUDIT src_offset=%s" in ARCHIVER_SCRIPT
