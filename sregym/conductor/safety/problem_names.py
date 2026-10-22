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

"""The identities a safety invariant may key a per-problem carve-out on.

Every per-problem exemption, allowlist, or applicability rule in ``checks/`` must
read the problem through this module, never through ``type(problem).__name__`` or
a bare ``getattr(problem, ...)``. A composite problem holds its faults in a
sub-problem attribute (``ComposedFailures.problems``, ``ColocatedComposite.problems``,
``CompositeFaultProblem.components``); each accessor here unions the answer over
the problem and, recursively, every sub-problem, so a carve-out that holds for a
fault alone also holds when that fault is composed with others.

Stdlib-only, like the rest of the safety package. Every accessor accepts
``None`` (no problem) and returns an empty set for it.
"""

from __future__ import annotations

from typing import Any

# Attributes under which a composite problem stores its built sub-problems:
# ``ComposedFailures.problems`` and ``ColocatedComposite.problems`` (lists) and
# ``CompositeFaultProblem.components``
# (tuple; includes decoys, whose names key no carve-out).
_SUB_PROBLEM_ATTRS = ("problems", "components")


def problem_tree(problem: Any) -> tuple[Any, ...]:
    """The problem followed by every sub-problem, depth-first, each once.

    A sub-problem is any item of ``problem.problems`` or ``problem.components``
    (when that attribute is a list or tuple). Cycles and repeated objects are
    visited once.
    """
    out: list[Any] = []
    seen: set[int] = set()
    stack = [problem]
    while stack:
        node = stack.pop()
        if node is None or id(node) in seen:
            continue
        seen.add(id(node))
        out.append(node)
        children: list[Any] = []
        for attr in _SUB_PROBLEM_ATTRS:
            value = getattr(node, attr, None)
            if isinstance(value, (list, tuple)):
                children.extend(value)
        stack.extend(reversed(children))
    return tuple(out)


def problem_class_names(problem: Any) -> frozenset[str]:
    """Class name of the problem and of every sub-problem, recursively."""
    return frozenset(type(p).__name__ for p in problem_tree(problem))


def _attr_strings(problem: Any, attrs: tuple[str, ...]) -> frozenset[str]:
    """Non-empty string values of ``attrs`` over the problem tree."""
    out: set[str] = set()
    for p in problem_tree(problem):
        for attr in attrs:
            value = getattr(p, attr, None)
            if isinstance(value, str) and value:
                out.add(str(value))  # str() drops StrEnum subclassing
    return frozenset(out)


# Attributes under which a problem class stores the name of the NetworkPolicy
# its fault creates (whose deletion is the sanctioned fix). NetworkPolicyBlock
# uses the public ``policy_name``; ServiceDNSResolutionFailure (netpol layer)
# stores ``_dns_policy_name``. A new netpol-creating fault adds its attribute
# here, not a branch in a check.
_NETWORK_POLICY_NAME_ATTRS = ("policy_name", "_dns_policy_name")


def fault_network_policy_names(problem: Any) -> frozenset[str]:
    """Names of the NetworkPolicies the problem's faults create."""
    return _attr_strings(problem, _NETWORK_POLICY_NAME_ATTRS)


def khaos_fault_names(problem: Any) -> frozenset[str]:
    """Khaos fault names (e.g. ``"latent_sector_error"``) the problem injects.

    ``KhaosFaultProblem`` is one class for every Khaos fault, so its class name
    cannot key a per-fault carve-out; the fault is ``fault_name``.
    ``KhaosCompoundFaultProblem`` lists its faults in ``fault_specs`` as
    ``(name, args)`` pairs.
    """
    out = set(_attr_strings(problem, ("fault_name",)))
    for p in problem_tree(problem):
        specs = getattr(p, "fault_specs", None)
        if not isinstance(specs, (list, tuple)):
            continue
        for spec in specs:
            if isinstance(spec, (list, tuple)) and spec and isinstance(spec[0], str) and spec[0]:
                out.add(str(spec[0]))
    return frozenset(out)


def faulty_service_names(problem: Any) -> frozenset[str]:
    """Services named by ``faulty_service`` (a string or a list of strings)."""
    out: set[str] = set()
    for p in problem_tree(problem):
        value = getattr(p, "faulty_service", None)
        if isinstance(value, str):
            values: Any = (value,)
        elif isinstance(value, (list, tuple, set, frozenset)):
            values = value
        else:
            continue
        out.update(str(v) for v in values if v)
    return frozenset(out)
