"""Base class for evaluation oracles."""

from abc import ABC, abstractmethod

from sregym.conductor.oracles.rollouts import (
    ROLLOUT_POLL_S,
    ROLLOUT_SETTLE_S,
    Settle,
    namespace_reader,
    wait_for_rollouts,
)


class Oracle(ABC):
    # Set True by a caller that already gave the namespace its settle window;
    # ``settle_rollouts`` then returns at once instead of every oracle waiting on the same
    # condition.
    namespace_settled: bool = False

    def __init__(self, problem):
        self.problem = problem

    # An optional hook with a do-nothing default, not abstract: most oracles already judge only
    # their own fault, and @abstractmethod would force each of them to restate the no-op.
    def scope_to_fault(self) -> None:  # noqa: B027
        """Judge only evidence attributable to this oracle's own fault, not the whole namespace.

        Called by ``CompositeFaultProblem`` on every fault component's oracle, where other faults
        share the namespace. Default: nothing to narrow (the oracle already checks only its own
        fault).
        """

    def settle_rollouts(
        self, *, timeout_s=ROLLOUT_SETTLE_S, poll_s=ROLLOUT_POLL_S, only=None, read=None
    ) -> Settle | None:
        """Wait (bounded) until nothing in the problem's namespace is still rolling out
        (``oracles/rollouts.py``: Deployments, StatefulSets, Terminating pods), or only the
        Deployment ``only`` and its pods, unless ``namespace_settled``. ``read`` defaults to
        reading through ``problem.kubectl``. On timeout it prints a warning and returns: the
        oracle judges the current state. Returns None when it skipped the wait."""
        if self.namespace_settled:
            return None
        if read is None:
            kubectl = self.problem.kubectl
            read = namespace_reader(kubectl.core_v1_api, kubectl.apps_v1_api, self.problem.namespace)
        settle = wait_for_rollouts(read, timeout_s, poll_s, only)
        if not settle.settled:
            print(f"⚠️ Namespace not settled ({settle.reason}); evaluating current state")
        return settle

    @abstractmethod
    def evaluate(self, solution, trace, duration) -> dict:
        """Evaluate a solution. The result has ``success``; a mitigation oracle that could not
        judge for a reason outside the agent's control (a cluster add-on it reads was down)
        also sets ``infra_error`` to a short slug, and the conductor records the row as an
        infra error rather than a grade."""
        pass
