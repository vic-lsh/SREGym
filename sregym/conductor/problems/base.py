"""Problem base class"""

from abc import ABC, abstractmethod
from collections.abc import Iterable


class Problem(ABC):
    # Upper bound, in seconds, on one stage evaluation (one oracle's evaluate()). The harness
    # waits this long for a pending evaluation before tearing the problem down (main.py).
    # Problems whose grading takes longer derive a larger value (composites, see
    # serial_evaluation_timeout_s and ColocatedComposite).
    evaluation_timeout_s: float = 300.0

    def __init__(self, app, namespace: str | None = None):
        self.app = app
        self.namespace = app.namespace if namespace is None else namespace
        self.fault_injected = False
        self.results = {}
        self.root_cause = None  # root cause of the problem in natural language

        # Optional: attach oracles in subclass
        self.diagnosis_oracle = None
        self.mitigation_oracle = None

    def requires_khaos(self) -> bool:
        """Override this method to return True if the problem requires Khaos for fault injection."""
        return False

    def injection_failure(self) -> dict | None:
        """After ``inject_fault``: None if the fault is in place, else fields saying why not.

        The conductor then runs no agent and grades nothing (``infra_error`` =
        ``injection_invalid``). A single fault's ``inject_fault`` raises on failure, so the
        default is None; a composite, which records per-fault errors, overrides it.
        """
        return None

    @classmethod
    def build_structured_root_cause(
        cls,
        *,
        component: str,
        namespace: str,
        description: str,
    ) -> str:
        """Return canonical structured root_cause text for judge-side parsing.

        Format:
        [fault_spec] component=<...>; namespace=<...> || <human-readable-description>
        """
        kv = [("component", component), ("namespace", namespace)]
        meta = "; ".join(f"{k}={str(v).strip()}" for k, v in kv)

        return f"[fault_spec] {meta} || {description.strip()}"

    @abstractmethod
    def inject_fault(self):
        pass

    @abstractmethod
    def recover_fault(self):
        pass


def serial_evaluation_timeout_s(problems: Iterable[Problem]) -> float:
    """Evaluation bound for a composite whose stage evaluates each sub-problem's oracle in turn.

    ``CompoundedOracle`` (and a per-fault diagnosis oracle) run the sub-oracles one after another,
    so the bound is the sum of the sub-problems' own bounds; never below the single-problem default.
    """
    return max(
        Problem.evaluation_timeout_s,
        sum(float(getattr(p, "evaluation_timeout_s", Problem.evaluation_timeout_s)) for p in problems),
    )
