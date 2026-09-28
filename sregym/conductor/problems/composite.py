"""Composite fault problems: several registered problems (and optional decoys) live at once on one app.

A composite is a named entry of ``composite_specs.json``. Its components are registry problem ids
(``role = "fault"``) or decoy kinds (``role = "decoy"``). The registry merges every entry, so the
conductor and the fast-loop worker build a composite exactly like any other problem.

Semantics:

- ``inject_fault`` injects the components in catalog order. If one fails, the components already
  injected are recovered (last first) and the injection error is raised.
- ``recover_fault`` recovers every component not yet recovered, last injected first. Every recovery
  is attempted even when an earlier one raises; the errors are then raised together as a
  ``CompositeRecoveryError``.
- The mitigation oracle is the AND of every fault component's own mitigation oracle
  (``CompoundedOracle``). Decoys are never graded: leaving one in place or reverting it are both correct.
- The diagnosis ground truth lists every fault component's root cause and names each decoy as not
  a root cause.
"""

from __future__ import annotations

import json
import re
import shlex
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from sregym.conductor.oracles.compound import CompoundedOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem

COMPOSITE_SPECS_PATH = Path(__file__).with_name("composite_specs.json")
COMPOSITE_PREFIX = "composite_"

ComponentRole = Literal["fault", "decoy"]
_ROLES = ("fault", "decoy")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_VALUE = re.compile(r"^[A-Za-z0-9_.:/-]*$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


class CompositeRecoveryError(ExceptionGroup):
    """One or more components of a composite failed to recover; every recovery was still attempted."""

    def __new__(cls, problem_id: str, errors: list[tuple[str, Exception]]):
        names = ", ".join(name for name, _ in errors)
        return super().__new__(cls, f"{problem_id}: recovery failed for {names}", [error for _, error in errors])

    def __init__(self, problem_id: str, errors: list[tuple[str, Exception]]) -> None:
        super().__init__(self.message, self.exceptions)
        self.problem_id = problem_id
        self.components = [name for name, _ in errors]

    def derive(self, excs):  # keep ExceptionGroup.split/subgroup working
        return ExceptionGroup(self.message, excs)


@dataclass(frozen=True)
class ComponentSpec:
    """One component of a composite: a registered fault problem, or a decoy kind with parameters."""

    role: ComponentRole
    problem: str | None = None
    decoy: str | None = None
    params: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.role not in _ROLES:
            raise ValueError(f"component role must be one of {_ROLES}, got {self.role!r}")
        if self.role == "fault" and (not self.problem or self.decoy is not None or self.params):
            raise ValueError("a fault component names a registry problem, and only that")
        if self.role == "decoy" and (not self.decoy or self.problem is not None):
            raise ValueError("a decoy component names a decoy kind (and optional params), not a problem")
        if not isinstance(self.params, Mapping):
            raise TypeError("component params must be a mapping")

    @property
    def name(self) -> str:
        return str(self.problem if self.role == "fault" else self.decoy)


@dataclass(frozen=True)
class CompositeSpec:
    """A named composite: its components in injection order."""

    problem_id: str
    components: tuple[ComponentSpec, ...]
    description: str = ""

    def __post_init__(self) -> None:
        if not self.problem_id.startswith(COMPOSITE_PREFIX):
            raise ValueError(f"composite ids start with {COMPOSITE_PREFIX!r}, got {self.problem_id!r}")
        if len(self.components) < 2:
            raise ValueError(f"{self.problem_id}: a composite has at least two components")
        if not any(component.role == "fault" for component in self.components):
            raise ValueError(f"{self.problem_id}: a composite has at least one fault component")
        keys = [
            (component.role, component.name, json.dumps(dict(component.params), sort_keys=True))
            for component in self.components
        ]
        if len(set(keys)) != len(keys):
            raise ValueError(f"{self.problem_id}: a component appears more than once")


_COMPONENT_KEYS = {"role", "problem", "decoy", "params"}
_COMPOSITE_KEYS = {"components", "description"}


def _check_keys(where: str, raw: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown keys {unknown}")


def load_composite_specs(path: Path = COMPOSITE_SPECS_PATH) -> dict[str, CompositeSpec]:
    """Parse the composite table; keys are composite problem ids, in file order."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: the composite table is a JSON object keyed by problem id")
    specs: dict[str, CompositeSpec] = {}
    for problem_id, entry in raw.items():
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: {problem_id} must be an object")
        _check_keys(f"{path}: {problem_id}", entry, _COMPOSITE_KEYS)
        components = []
        for position, component in enumerate(entry.get("components", [])):
            if not isinstance(component, dict):
                raise ValueError(f"{path}: {problem_id} component {position} must be an object")
            _check_keys(f"{path}: {problem_id} component {position}", component, _COMPONENT_KEYS)
            components.append(ComponentSpec(**component))
        specs[problem_id] = CompositeSpec(
            problem_id=problem_id,
            components=tuple(components),
            description=str(entry.get("description", "")),
        )
    return specs


class BenignEnvDrift(Problem):
    """Decoy: set harmless environment variables on a Deployment, which rolls it out.

    Hotel reservation's ``LOG_LEVEL`` only changes zerolog's level (``tune/setting.go``), so the drift
    shows up in the state diff and in the logs without breaking anything. Recovery restores each
    variable's previous value, or removes it if it was absent.
    """

    ROLLOUT_TIMEOUT = "180s"

    def __init__(self, app, *, deployment: str, env: Mapping[str, str], kubectl=None) -> None:
        if not isinstance(deployment, str) or not _DNS_LABEL.match(deployment):
            raise ValueError(f"benign_env_drift needs a deployment name, got {deployment!r}")
        if not env:
            raise ValueError("benign_env_drift sets at least one variable")
        for name, value in env.items():
            if not isinstance(name, str) or not _ENV_NAME.match(name):
                raise ValueError(f"benign_env_drift: invalid variable name {name!r}")
            if not isinstance(value, str) or not _ENV_VALUE.match(value):
                raise ValueError(f"benign_env_drift: invalid value {value!r} for {name}")
        super().__init__(app=app)
        if kubectl is None:
            from sregym.service.kubectl import KubeCtl

            kubectl = KubeCtl()
        self.kubectl = kubectl
        self.deployment = deployment
        self.env = dict(env)
        self._previous: dict[str, str | None] | None = None
        assignments = ", ".join(f"`{name}={value}`" for name, value in self.env.items())
        self.root_cause = (
            f"Decoy, not a root cause: the deployment `{deployment}` has a recent, harmless environment change "
            f"({assignments}). It only changes log verbosity; the service behaves the same with or without it."
        )
        self.mitigation_oracle = None

    def _deployment_ref(self) -> str:
        return f"deployment/{self.deployment} -n {shlex.quote(self.namespace)}"

    def _current_env(self) -> dict[str, str | None]:
        raw = self.kubectl.exec_command(
            f"kubectl get deployment {shlex.quote(self.deployment)} -n {shlex.quote(self.namespace)} -o json"
        )
        containers = json.loads(raw)["spec"]["template"]["spec"]["containers"]
        values: dict[str, str | None] = dict.fromkeys(self.env)
        for item in containers[0].get("env") or []:
            if item.get("name") in values:
                values[item["name"]] = item.get("value")
        return values

    def _rollout(self) -> None:
        self.kubectl.exec_command(f"kubectl rollout status {self._deployment_ref()} --timeout={self.ROLLOUT_TIMEOUT}")

    def inject_fault(self) -> None:
        self._previous = self._current_env()
        assignments = " ".join(f"{name}={value}" for name, value in self.env.items())
        self.kubectl.exec_command(f"kubectl set env {self._deployment_ref()} {assignments}")
        self._rollout()
        self.fault_injected = True

    def recover_fault(self) -> None:
        if self._previous is None:
            return
        restored = " ".join(f"{name}={value}" for name, value in self._previous.items() if value is not None)
        removed = " ".join(f"{name}-" for name, value in self._previous.items() if value is None)
        for change in (restored, removed):
            if change:
                self.kubectl.exec_command(f"kubectl set env {self._deployment_ref()} {change}")
        self._rollout()
        self._previous = None
        self.fault_injected = False


DecoyFactory = Callable[..., Problem]
DECOY_FACTORIES: dict[str, DecoyFactory] = {"benign_env_drift": BenignEnvDrift}


def _raw(method: Callable[[], Any]) -> Callable[[], Any]:
    """The undecorated method: ``mark_fault_injected`` turns recovery errors into warnings."""

    wrapped = getattr(method, "__wrapped__", None)
    owner = getattr(method, "__self__", None)
    if wrapped is None or owner is None:
        return method
    return lambda: wrapped(owner)


class CompositeFaultProblem(Problem):
    """Several components live at once on one application; see the module docstring."""

    def __init__(
        self,
        spec: CompositeSpec,
        *,
        component_factory: Callable[[str], Problem],
        decoy_factories: Mapping[str, DecoyFactory] = DECOY_FACTORIES,
    ) -> None:
        self.spec = spec
        self.problem_id = spec.problem_id
        faults = {
            index: component_factory(str(component.problem))
            for index, component in enumerate(spec.components)
            if component.role == "fault"
        }
        app = faults[min(faults)].app
        components: list[Problem] = []
        for index, component in enumerate(spec.components):
            if component.role == "fault":
                components.append(faults[index])
                continue
            if component.decoy not in decoy_factories:
                raise ValueError(f"{spec.problem_id}: unknown decoy kind {component.decoy!r}")
            components.append(decoy_factories[str(component.decoy)](app, **dict(component.params)))
        namespaces = {part.namespace for part in components}
        if len(namespaces) != 1:
            raise ValueError(f"{spec.problem_id}: components must share one application namespace, got {namespaces}")
        super().__init__(app=app)
        self.components: tuple[Problem, ...] = tuple(components)
        self.fault_indices: tuple[int, ...] = tuple(sorted(faults))
        self._injected: list[int] = []
        self._recovered: set[int] = set()

        for index in self.fault_indices:
            if self.components[index].mitigation_oracle is None:
                raise ValueError(f"{spec.problem_id}: fault {spec.components[index].name} has no mitigation oracle")
        self.mitigation_oracle = CompoundedOracle(
            self,
            **{self._key(index): self.components[index].mitigation_oracle for index in self.fault_indices},
        )
        self.root_cause = self._ground_truth()
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

    def _key(self, index: int) -> str:
        return f"{index}-{self.spec.components[index].name}"

    def _ground_truth(self) -> str:
        causes = [
            f"Root cause {number} ({self.spec.components[index].name}):\n{(self.components[index].root_cause or '').strip()}"
            for number, index in enumerate(self.fault_indices, start=1)
        ]
        decoys = [
            f"Decoy ({component.name}), NOT a root cause:\n{(self.components[index].root_cause or '').strip()}"
            for index, component in enumerate(self.spec.components)
            if component.role == "decoy"
        ]
        text = (
            f"This incident has {len(causes)} simultaneous root cause(s); a correct diagnosis names every one of "
            "them.\n\n" + "\n\n".join(causes)
        )
        if decoys:
            text += (
                "\n\nThe following change is also present but is NOT a root cause; a diagnosis that names it as "
                "the cause is wrong.\n\n" + "\n\n".join(decoys)
            )
        return text

    def requires_khaos(self) -> bool:
        return any(component.requires_khaos() for component in self.components)

    def inject_fault(self) -> None:
        self._injected, self._recovered = [], set()
        for index, component in enumerate(self.components):
            print(f"== Composite {self.problem_id}: injecting {self.spec.components[index].name} ==")
            try:
                component.inject_fault()
            except Exception as error:
                try:
                    self.recover_fault()
                except CompositeRecoveryError as rollback:
                    error.add_note(f"rollback of the injected components also failed: {rollback}")
                raise
            self._injected.append(index)
        self.fault_injected = True

    def _recover(self, index: int) -> None:
        print(f"== Composite {self.problem_id}: recovering {self.spec.components[index].name} ==")
        component = self.components[index]
        _raw(component.recover_fault)()
        component.fault_injected = False
        self._recovered.add(index)

    def recover_component(self, index: int) -> None:
        """Recover one injected component; the others stay live. Errors are raised, not swallowed."""

        if index not in self._injected:
            raise ValueError(f"{self.problem_id}: component {index} is not injected")
        if index in self._recovered:
            raise ValueError(f"{self.problem_id}: component {index} is already recovered")
        self._recover(index)

    @property
    def pending_components(self) -> tuple[int, ...]:
        """Injected components not recovered yet, in recovery order (last injected first)."""

        return tuple(index for index in reversed(self._injected) if index not in self._recovered)

    def recover_fault(self) -> None:
        errors: list[tuple[str, Exception]] = []
        for index in self.pending_components:
            try:
                self._recover(index)
            except Exception as error:
                errors.append((self.spec.components[index].name, error))
        self.fault_injected = False
        if errors:
            raise CompositeRecoveryError(self.problem_id, errors)

    def component_oracle(self, index: int):
        """The mitigation oracle of one fault component."""

        if index not in self.fault_indices:
            raise ValueError(f"{self.problem_id}: component {index} is not a graded fault")
        return self.components[index].mitigation_oracle


def composite_factories(
    component_factory: Callable[[str], Problem], path: Path = COMPOSITE_SPECS_PATH
) -> dict[str, Callable[[], CompositeFaultProblem]]:
    """One zero-argument factory per composite in the table, for the problem registry."""

    return {
        problem_id: (lambda spec=spec: CompositeFaultProblem(spec, component_factory=component_factory))
        for problem_id, spec in load_composite_specs(path).items()
    }
