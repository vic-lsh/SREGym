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
"""Is each injected fault still in effect after every fault was injected?

Pure module: standard library only. The composite (the I/O shell) lists the objects of
every kind in `NAMESPACED_KINDS` and `CLUSTER_KINDS` before and after each injection and
hands the raw API JSON to `build_snapshot`. This module then:

1. normalizes each object (drops status and server-owned bookkeeping, hashes Secret
   values, expands structured ConfigMap data such as a flagd JSON/YAML document) and
   flattens it to `path -> leaf value`;
2. diffs the snapshots around one injection: that fault's `Change`s;
3. after all injections, decides liveness: a fault is live if at least one of its
   changes still holds its post-injection value in the final snapshot (`spec_verdict`).
   A fault with no spec change (runtime-only: a dropped Mongo user, a Valkey password,
   a sysctl) cannot be judged from specs; the shell runs that fault's own mitigation
   oracle once and passes the result to `oracle_verdict` ("oracle fails" means live).

Values never leave this module except as `Change` objects; reports carry paths only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# Kinds listed in the app namespace, one list call each per snapshot.
NAMESPACED_KINDS: tuple[str, ...] = (
    "Deployment",
    "StatefulSet",
    "DaemonSet",
    "Service",
    "ConfigMap",
    "Secret",
    "NetworkPolicy",
    "PersistentVolumeClaim",
    "ServiceAccount",
    "Role",
    "RoleBinding",
    "ResourceQuota",
    "LimitRange",
)
# The app Namespace object itself (labels only: PSA labels live here).
NAMESPACE_KIND = "Namespace"
# Cluster-scoped kinds that can block or rewrite pod admission in the app namespace.
CLUSTER_KINDS: tuple[str, ...] = (
    "ValidatingWebhookConfiguration",
    "MutatingWebhookConfiguration",
)
ALL_KINDS: tuple[str, ...] = NAMESPACED_KINDS + (NAMESPACE_KIND,) + CLUSTER_KINDS

BASIS_SPEC_DIFF = "spec_diff"
BASIS_ORACLE = "oracle"
BASIS_NONE = "none"


class _Absent:
    """The value of a path that does not exist in a snapshot."""

    _instance: _Absent | None = None

    def __new__(cls) -> _Absent:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "ABSENT"


ABSENT = _Absent()

# --- normalization -----------------------------------------------------------------

_DROP_METADATA = frozenset(
    {
        "namespace",
        "resourceVersion",
        "generation",
        "uid",
        "managedFields",
        "creationTimestamp",
        "selfLink",
        "deletionGracePeriodSeconds",
    }
)
# Annotation keys written by kubectl, controllers, or the volume binder, never by a fault.
_NOISE_ANNOTATIONS = frozenset(
    {
        "kubectl.kubernetes.io/last-applied-configuration",  # may also embed Secret values
        "kubectl.kubernetes.io/restartedAt",  # every `rollout restart`, fault or not
        "deployment.kubernetes.io/revision",
        "deprecated.daemonset.template.generation",
        "endpoints.kubernetes.io/last-change-trigger-time",
        "control-plane.alpha.kubernetes.io/leader",
    }
)
_NOISE_ANNOTATION_PREFIXES = ("pv.kubernetes.io/", "volume.kubernetes.io/", "volume.beta.kubernetes.io/")
# Label keys added by controllers to pods/ReplicaSets and occasionally echoed into selectors.
_NOISE_LABELS = frozenset({"pod-template-hash", "controller-revision-hash", "statefulset.kubernetes.io/pod-name"})
# Per-kind fields allocated or filled in by the API server (re-allocated when an object is
# recreated), as dotted paths; `*` matches every list element.
_DROP_FIELDS: Mapping[str, tuple[str, ...]] = {
    "Service": ("spec.clusterIP", "spec.clusterIPs", "spec.healthCheckNodePort", "spec.ports.*.nodePort"),
    "PersistentVolumeClaim": ("spec.volumeName",),
    "ServiceAccount": ("secrets",),
}
_SECRET_VALUE_FIELDS = ("data", "stringData")

# Parses one ConfigMap data value (text) into a document; raises or returns a non-dict when the
# text is not a structured document. The default handles JSON; the shell passes a YAML parser
# (YAML is not in the standard library).
TextParser = Callable[[str], Any]


def _expand_config_data(data: dict[str, Any], parse_text: TextParser) -> None:
    """Replace each ConfigMap data value that parses to a non-empty mapping by that mapping.

    Several faults edit different keys inside ONE data value (astronomy_shop's
    `demo.flagd.json`, train_ticket's `flags.yaml`: one flag each). Compared as an opaque
    string, a later fault's edit made every earlier one look reverted (live #150); expanded,
    each fault's change is its own leaf paths.
    """
    for key, value in list(data.items()):
        if not isinstance(value, str):
            continue
        try:
            parsed = parse_text(value)
        except Exception:  # not a document: keep the text
            continue
        if isinstance(parsed, dict) and parsed:
            data[key] = json.loads(json.dumps(parsed, default=str))  # JSON-typed leaves


def _secret_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _clean_map(mapping: Any, noise: frozenset[str], prefixes: tuple[str, ...] = ()) -> Any:
    if not isinstance(mapping, dict):
        return mapping
    return {k: v for k, v in mapping.items() if k not in noise and not k.startswith(prefixes)}


def _drop_path(obj: Any, parts: list[str]) -> None:
    if not parts:
        return
    head, rest = parts[0], parts[1:]
    if head == "*":
        if isinstance(obj, list):
            for item in obj:
                _drop_path(item, rest)
        return
    if not isinstance(obj, dict) or head not in obj:
        return
    if rest:
        _drop_path(obj[head], rest)
    else:
        del obj[head]


def _scrub(value: Any, parent_key: str | None = None) -> Any:
    """Recursively drop nulls, noise labels/annotations, and nested metadata bookkeeping."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if v is None:
                continue
            if k == "annotations" and parent_key == "metadata":
                v = _clean_map(v, _NOISE_ANNOTATIONS, _NOISE_ANNOTATION_PREFIXES)
                if not v:  # `kubectl apply` adding only last-applied is not a change
                    continue
            elif k in ("labels", "matchLabels") or (k == "selector" and parent_key == "spec"):
                v = _clean_map(v, _NOISE_LABELS)
            elif k == "metadata" and isinstance(v, dict):
                v = {mk: mv for mk, mv in v.items() if mk not in _DROP_METADATA}
            elif k == "ownerReferences" and isinstance(v, list):
                v = [{rk: rv for rk, rv in ref.items() if rk != "uid"} if isinstance(ref, dict) else ref for ref in v]
            out[k] = _scrub(v, k)
        return out
    if isinstance(value, list):
        return [_scrub(v, parent_key) for v in value]
    return value


def normalize(kind: str, obj: Mapping[str, Any], parse_text: TextParser = json.loads) -> dict[str, Any]:
    """Return the fault-relevant part of one API object (JSON dict) of `kind`.

    Drops `status`, `apiVersion`/`kind`, server bookkeeping in metadata (uid,
    resourceVersion, generation, managedFields, creationTimestamp, ...), kubectl and
    controller annotations, pod-template-hash style labels, and server-allocated fields
    (Service clusterIP/nodePort, PVC volumeName). Secret values are replaced by a short
    digest. ConfigMap `data` values that `parse_text` turns into a mapping are expanded into
    it. The Namespace object keeps only its labels. Does not mutate `obj`.
    """
    data = json.loads(json.dumps(obj, default=str))  # deep copy, JSON-typed
    for key in ("status", "apiVersion", "kind"):
        data.pop(key, None)
    if kind == NAMESPACE_KIND:
        labels = (data.get("metadata") or {}).get("labels") or {}
        return {"metadata": {"labels": labels}}
    if kind == "Secret":
        for field in _SECRET_VALUE_FIELDS:
            if isinstance(data.get(field), dict):
                data[field] = {k: _secret_digest(v) for k, v in data[field].items()}
    for path in _DROP_FIELDS.get(kind, ()):
        _drop_path(data, path.split("."))
    data = _scrub(data)
    if kind == "ConfigMap" and isinstance(data.get("data"), dict):
        _expand_config_data(data["data"], parse_text)
    meta = data.get("metadata")
    if isinstance(meta, dict):
        meta.pop("name", None)  # the name is part of every path already
        if not meta:
            del data["metadata"]
    return data


# --- flattening --------------------------------------------------------------------

_PLAIN_SEGMENT = re.compile(r"[A-Za-z0-9_\-]+")


def _segment(key: str) -> str:
    return "." + key if _PLAIN_SEGMENT.fullmatch(key) else "[" + json.dumps(key) + "]"


def _named_list(items: list[Any]) -> bool:
    names = [i.get("name") for i in items if isinstance(i, dict)]
    return (
        bool(items)
        and len(names) == len(items)
        and all(isinstance(n, str) for n in names)
        and len(set(names)) == len(names)
    )


def _flatten(value: Any, prefix: str, out: dict[str, Any]) -> None:
    if isinstance(value, dict) and value:
        for k in sorted(value):
            _flatten(value[k], prefix + _segment(str(k)), out)
    elif isinstance(value, list) and value and all(isinstance(i, dict) for i in value):
        if _named_list(value):  # containers, env, ports, volumes: stable under insertion
            for item in value:
                _flatten(item, f"{prefix}[name={json.dumps(item['name'])}]", out)
        else:
            for idx, item in enumerate(value):
                _flatten(item, f"{prefix}[{idx}]", out)
    elif isinstance(value, (dict, list)):
        out[prefix] = json.dumps(value, sort_keys=True)  # empty containers, scalar lists
    else:
        out[prefix] = value


def object_key(kind: str, name: str) -> str:
    return f"{kind}/{json.dumps(name)}"


def kind_of(path: str) -> str:
    return path.split("/", 1)[0]


@dataclass(frozen=True)
class Snapshot:
    """A flattened view of every listed object: `path -> leaf value`.

    `kinds` are the kinds whose list call succeeded; paths of other kinds are unknown
    (not absent), so diffs and verdicts ignore them.
    """

    values: Mapping[str, Any]
    kinds: frozenset[str]

    def get(self, path: str) -> Any:
        return self.values.get(path, ABSENT)


def build_snapshot(raw: Mapping[str, Iterable[Mapping[str, Any]]], parse_text: TextParser = json.loads) -> Snapshot:
    """Normalize and flatten `kind -> [API object JSON, ...]`.

    A kind missing from `raw` was not readable. Unknown kinds raise ValueError.
    `parse_text` parses ConfigMap data values (see `normalize`).
    """
    unknown = sorted(set(raw) - set(ALL_KINDS))
    if unknown:
        raise ValueError(f"build_snapshot: unknown kinds {unknown}")
    values: dict[str, Any] = {}
    for kind, objects in raw.items():
        for obj in objects:
            name = (obj.get("metadata") or {}).get("name")
            if not isinstance(name, str):
                raise ValueError(f"build_snapshot: {kind} object without metadata.name")
            _flatten(normalize(kind, obj, parse_text), object_key(kind, name), values)
    return Snapshot(values=values, kinds=frozenset(raw))


# --- diff and verdicts ---------------------------------------------------------------


@dataclass(frozen=True)
class Change:
    """One path whose value differs across an injection. ABSENT marks add/remove."""

    path: str
    before: Any
    after: Any

    @property
    def op(self) -> str:
        if self.before is ABSENT:
            return "+"
        if self.after is ABSENT:
            return "-"
        return "~"


def _same(a: Any, b: Any) -> bool:
    if a is ABSENT or b is ABSENT:
        return a is b
    return type(a) is type(b) and a == b  # 1 != True here


def diff(before: Snapshot, after: Snapshot) -> tuple[Change, ...]:
    """Changed, added, and removed paths, restricted to kinds readable in both snapshots."""
    kinds = before.kinds & after.kinds
    paths = {p for p in before.values if kind_of(p) in kinds} | {p for p in after.values if kind_of(p) in kinds}
    changes = []
    for path in sorted(paths):
        b, a = before.get(path), after.get(path)
        if not _same(b, a):
            changes.append(Change(path, b, a))
    return tuple(changes)


def still_holding(changes: Iterable[Change], final: Snapshot) -> tuple[Change, ...]:
    """The changes whose post-injection value is still the value in `final`."""
    out = []
    for c in changes:
        if kind_of(c.path) not in final.kinds:
            continue
        if _same(final.get(c.path), c.after):
            out.append(c)
    return tuple(out)


@dataclass(frozen=True)
class LivenessVerdict:
    live: bool
    basis: str  # BASIS_SPEC_DIFF | BASIS_ORACLE | BASIS_NONE
    detail: str


def spec_verdict(changes: tuple[Change, ...] | None, final: Snapshot) -> LivenessVerdict | None:
    """Judge liveness from the fault's spec diff, or None when specs cannot tell.

    None when the diff is unknown (a snapshot failed), empty (a runtime-only fault), or
    every changed kind was unreadable in `final`; the caller then falls back to the oracle.
    """
    if not changes:
        return None
    judgeable = [c for c in changes if kind_of(c.path) in final.kinds]
    if not judgeable:
        return None
    holding = still_holding(judgeable, final)
    if holding:
        return LivenessVerdict(
            True, BASIS_SPEC_DIFF, f"{len(holding)}/{len(judgeable)} changes hold, e.g. {holding[0].path}"
        )
    return LivenessVerdict(False, BASIS_SPEC_DIFF, f"all {len(judgeable)} changes reverted, e.g. {judgeable[0].path}")


def oracle_verdict(oracle_success: bool | None, error: str = "") -> LivenessVerdict:
    """Fallback for faults without a spec diff: the fault's own mitigation oracle.

    `oracle_success` False (the oracle still sees the fault) means live. None means the
    oracle is missing or crashed: liveness is unproven, so the fault counts as not live.
    """
    if oracle_success is None:
        return LivenessVerdict(False, BASIS_NONE, error or "no spec change and no oracle verdict")
    if oracle_success:
        return LivenessVerdict(False, BASIS_ORACLE, "no spec change and the mitigation oracle already passes")
    return LivenessVerdict(True, BASIS_ORACLE, "no spec change; the mitigation oracle fails")


def summarize(changes: Iterable[Change], limit: int = 5) -> list[str]:
    """Up to `limit` "<op> <path>" strings (paths only, never values)."""
    return [f"{c.op} {c.path}" for c in list(changes)[:limit]]
