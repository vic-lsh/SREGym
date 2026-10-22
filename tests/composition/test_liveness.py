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
"""Pure liveness logic: normalize, diff, and still-holds over plain API-object dicts."""

import copy
import random

import pytest

from sregym.conductor.problems.composition import liveness as lv


def _service(name="user", target_port=8086, cluster_ip="10.0.0.1", uid="u1", rv="100"):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": "hotel",
            "uid": uid,
            "resourceVersion": rv,
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "managedFields": [{"manager": "kubectl"}],
            "labels": {"app": name},
            "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{...}"},
        },
        "spec": {
            "clusterIP": cluster_ip,
            "clusterIPs": [cluster_ip],
            "selector": {"io.kompose.service": name},
            "ports": [{"name": "http", "port": 8086, "targetPort": target_port, "protocol": "TCP"}],
        },
        "status": {"loadBalancer": {}},
    }


def _deployment(name="geo", image="geo:1", env=None, restarted_at=None, generation=1):
    template_meta = {"labels": {"app": name, "pod-template-hash": "abc"}}
    if restarted_at:
        template_meta["annotations"] = {"kubectl.kubernetes.io/restartedAt": restarted_at}
    return {
        "metadata": {
            "name": name,
            "generation": generation,
            "annotations": {"deployment.kubernetes.io/revision": str(generation)},
        },
        "spec": {
            "replicas": 1,
            "template": {
                "metadata": template_meta,
                "spec": {"containers": [{"name": name, "image": image, "env": env or []}]},
            },
        },
        "status": {"readyReplicas": 1},
    }


def _snap(**kinds):
    return lv.build_snapshot(kinds)


def test_normalize_drops_server_bookkeeping_and_keeps_fault_fields():
    norm = lv.normalize("Service", _service())

    assert norm == {
        "metadata": {"labels": {"app": "user"}},
        "spec": {
            "selector": {"io.kompose.service": "user"},
            "ports": [{"name": "http", "port": 8086, "targetPort": 8086, "protocol": "TCP"}],
        },
    }


def test_normalize_drops_restart_and_revision_noise():
    a = lv.normalize("Deployment", _deployment(generation=1))
    b = lv.normalize("Deployment", _deployment(generation=7, restarted_at="2026-10-17T00:00:00Z"))

    assert a == b


def test_normalize_does_not_mutate_input():
    obj = _service()
    before = copy.deepcopy(obj)

    lv.normalize("Service", obj)

    assert obj == before


def test_secret_values_are_hashed_never_kept():
    secret = {"metadata": {"name": "db"}, "data": {"password": "c3VwZXJzZWNyZXQ="}, "type": "Opaque"}

    snap = _snap(Secret=[secret])

    assert all("c3VwZXJzZWNyZXQ=" not in repr(v) for v in snap.values.values())
    assert any(str(v).startswith("sha256:") for v in snap.values.values())


def test_secret_value_change_is_still_detected():
    before = _snap(Secret=[{"metadata": {"name": "db"}, "data": {"password": "YQ=="}}])
    after = _snap(Secret=[{"metadata": {"name": "db"}, "data": {"password": "Yg=="}}])

    (change,) = lv.diff(before, after)

    assert change.op == "~"
    assert "password" in change.path


def test_namespace_object_keeps_only_labels():
    ns = {
        "metadata": {"name": "hotel", "uid": "x", "labels": {"pod-security.kubernetes.io/enforce": "restricted"}},
        "spec": {"finalizers": ["kubernetes"]},
        "status": {"phase": "Active"},
    }

    assert lv.normalize("Namespace", ns) == {
        "metadata": {"labels": {"pod-security.kubernetes.io/enforce": "restricted"}}
    }


def test_recreated_object_with_new_server_fields_has_no_diff():
    before = _snap(Service=[_service(cluster_ip="10.0.0.1", uid="a", rv="1")])
    after = _snap(Service=[_service(cluster_ip="10.0.0.9", uid="b", rv="2")])

    assert lv.diff(before, after) == ()


def test_diff_reports_changed_added_and_removed_paths():
    before = _snap(Service=[_service("user"), _service("rate")])
    after = _snap(Service=[_service("user", target_port=9999), _service("search")])

    ops = {(c.op, c.path.split('"')[1]) for c in lv.diff(before, after)}

    assert ops == {("~", "user"), ("-", "rate"), ("+", "search")}


def test_named_list_insertion_changes_only_the_new_entry():
    before = _snap(Deployment=[_deployment(env=[{"name": "A", "value": "1"}])])
    after = _snap(Deployment=[_deployment(env=[{"name": "Z", "value": "0"}, {"name": "A", "value": "1"}])])

    changes = lv.diff(before, after)

    assert changes
    assert all(c.op == "+" and 'env[name="Z"]' in c.path for c in changes)


def test_type_change_is_a_change():
    before = _snap(ConfigMap=[{"metadata": {"name": "c"}, "data": {"k": 1}}])
    after = _snap(ConfigMap=[{"metadata": {"name": "c"}, "data": {"k": True}}])

    assert len(lv.diff(before, after)) == 1


def test_unreadable_kind_is_excluded_not_treated_as_removed():
    before = _snap(Service=[_service()], ConfigMap=[{"metadata": {"name": "c"}, "data": {"k": "v"}}])
    after = _snap(Service=[_service()])  # ConfigMap list failed

    assert lv.diff(before, after) == ()


def test_build_snapshot_rejects_unknown_kind_and_nameless_objects():
    with pytest.raises(ValueError, match="unknown kinds"):
        lv.build_snapshot({"Pod": []})
    with pytest.raises(ValueError, match="metadata.name"):
        lv.build_snapshot({"Service": [{"spec": {}}]})


# --- verdicts ---------------------------------------------------------------------


def test_fault_erased_by_a_later_injection_is_not_live():
    base = _snap(Service=[_service(target_port=8086)])
    after_a = _snap(Service=[_service(target_port=9999)])
    after_b = _snap(Service=[_service(target_port=8086)])  # a later fault re-applied the original

    verdict = lv.spec_verdict(lv.diff(base, after_a), after_b)

    assert verdict == lv.LivenessVerdict(False, lv.BASIS_SPEC_DIFF, verdict.detail)


def test_fault_live_if_any_change_still_holds():
    base = _snap(Service=[_service(target_port=8086)], Deployment=[_deployment(image="geo:1")])
    after = _snap(Service=[_service(target_port=9999)], Deployment=[_deployment(image="geo:bad")])
    final = _snap(Service=[_service(target_port=8086)], Deployment=[_deployment(image="geo:bad")])

    verdict = lv.spec_verdict(lv.diff(base, after), final)

    assert verdict.live and verdict.basis == lv.BASIS_SPEC_DIFF


def test_deleted_object_stays_live_while_absent():
    base = _snap(Service=[_service("rate")])
    after = _snap(Service=[])

    assert lv.spec_verdict(lv.diff(base, after), after).live
    assert not lv.spec_verdict(lv.diff(base, after), base).live


def test_runtime_only_fault_defers_to_the_oracle():
    snap = _snap(Service=[_service()])

    assert lv.spec_verdict(lv.diff(snap, snap), snap) is None
    assert lv.spec_verdict(None, snap) is None
    assert lv.oracle_verdict(False) == lv.LivenessVerdict(True, lv.BASIS_ORACLE, lv.oracle_verdict(False).detail)
    assert not lv.oracle_verdict(True).live
    assert lv.oracle_verdict(None, "boom").basis == lv.BASIS_NONE
    assert not lv.oracle_verdict(None).live


def test_changes_to_kinds_unreadable_at_the_end_defer_to_the_oracle():
    base = _snap(ConfigMap=[{"metadata": {"name": "c"}, "data": {"k": "v"}}])
    after = _snap(ConfigMap=[{"metadata": {"name": "c"}, "data": {"k": "bad"}}])
    final = _snap(Service=[])

    assert lv.spec_verdict(lv.diff(base, after), final) is None


def test_summarize_lists_paths_only():
    before = _snap(Secret=[{"metadata": {"name": "s"}, "data": {"p": "YQ=="}}])
    after = _snap(Secret=[])

    (line,) = lv.summarize(lv.diff(before, after))

    assert line.startswith("- Secret/")
    assert "YQ==" not in line


# --- properties over generated objects --------------------------------------------


def _random_value(rng, depth):
    roll = rng.random()
    if depth <= 0 or roll < 0.35:
        return rng.choice([0, 1, True, False, "a", "b", "x.y", None, 2.5])
    if roll < 0.6:
        return [_random_value(rng, depth - 1) for _ in range(rng.randint(0, 3))]
    if roll < 0.8:
        names = rng.sample(["c1", "c2", "c3", "c4"], rng.randint(1, 3))
        return [{"name": n, "v": _random_value(rng, depth - 1)} for n in names]
    return {
        rng.choice(["a", "b", "c.d", "e/f", "spec"]): _random_value(rng, depth - 1) for _ in range(rng.randint(0, 3))
    }


def _random_snapshot(rng):
    raw = {}
    for kind in rng.sample(list(lv.NAMESPACED_KINDS), 3):
        raw[kind] = [
            {"metadata": {"name": n}, "spec": _random_value(rng, 3)}
            for n in rng.sample(["a", "b", "c"], rng.randint(0, 3))
        ]
    return raw


@pytest.mark.parametrize("seed", range(200))
def test_property_diff_holds_against_its_own_after_and_reverts_against_before(seed):
    rng = random.Random(seed)
    raw_a = _random_snapshot(rng)
    raw_b = {k: v for k, v in _random_snapshot(rng).items()}
    raw_b.update({k: v for k, v in raw_a.items() if k not in raw_b and rng.random() < 0.5})
    a, b = lv.build_snapshot(raw_a), lv.build_snapshot(raw_b)

    changes = lv.diff(a, b)

    assert lv.diff(a, a) == ()
    assert lv.still_holding(changes, b) == changes
    assert lv.still_holding(changes, a) == ()
    assert {c.path for c in changes} == {c.path for c in lv.diff(b, a)}


def _flagd(flags: dict) -> dict:
    import json

    doc = {"flags": {name: {"state": "ENABLED", "defaultVariant": v} for name, v in flags.items()}}
    return {"metadata": {"name": "flagd-config"}, "data": {"demo.flagd.json": json.dumps(doc, indent=2)}}


def test_edits_to_different_flags_in_one_json_value_stay_separately_live():
    # Regression (live #150): three flag faults on astronomy_shop's flagd-config were reported
    # "all changes reverted" because a later flag edit changed the same data string.
    base = _snap(ConfigMap=[_flagd({"a": "off", "b": "off"})])
    after_a = _snap(ConfigMap=[_flagd({"a": "on", "b": "off"})])
    after_b = _snap(ConfigMap=[_flagd({"a": "on", "b": "on"})])

    changes_a = lv.diff(base, after_a)

    assert [c.path for c in changes_a] == ['ConfigMap/"flagd-config".data["demo.flagd.json"].flags.a.defaultVariant']
    assert lv.spec_verdict(changes_a, after_b).live
    assert not lv.spec_verdict(changes_a, base).live  # reverting flag a is still seen


def test_config_data_uses_the_given_parser_and_keeps_unparsable_text():
    def parse_kv(text):  # stands in for yaml.safe_load: "k=v" lines -> mapping
        pairs = [line.split("=", 1) for line in text.splitlines() if line]
        if not all(len(p) == 2 for p in pairs):
            raise ValueError("not k=v")
        return dict(pairs)

    cm = {"metadata": {"name": "c"}, "data": {"flags.yaml": "x=1\ny=2", "plain": "hello world", "num": "7"}}

    assert lv.normalize("ConfigMap", cm, parse_kv)["data"] == {
        "flags.yaml": {"x": "1", "y": "2"},
        "plain": "hello world",
        "num": "7",
    }
    assert lv.normalize("ConfigMap", cm)["data"]["num"] == "7"  # json.loads -> 7, not a mapping: kept as text
