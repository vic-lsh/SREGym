from types import SimpleNamespace

from sregym.conductor.problems.base import Problem, serial_evaluation_timeout_s
from sregym.utils.decorators import mark_fault_injected


class ExampleProblem(Problem):
    @mark_fault_injected
    def inject_fault(self):
        pass

    @mark_fault_injected
    def recover_fault(self):
        pass


def test_namespace_defaults_to_application_namespace():
    app = SimpleNamespace(namespace="application-namespace")

    problem = ExampleProblem(app=app)

    assert problem.app is app
    assert problem.namespace == "application-namespace"


def test_explicit_namespace_overrides_application_namespace():
    app = SimpleNamespace(namespace="application-namespace")

    problem = ExampleProblem(app=app, namespace="custom-namespace")

    assert problem.namespace == "custom-namespace"


def test_explicit_empty_namespace_is_preserved():
    app = SimpleNamespace(namespace="application-namespace")

    problem = ExampleProblem(app=app, namespace="")

    assert problem.namespace == ""


def test_evaluation_timeout_defaults_to_300_seconds():
    assert ExampleProblem(app=SimpleNamespace(namespace="ns")).evaluation_timeout_s == 300.0


def test_serial_evaluation_timeout_is_a_sum_never_below_the_default():
    fast = SimpleNamespace(evaluation_timeout_s=10.0)
    slow = SimpleNamespace(evaluation_timeout_s=500.0)

    assert serial_evaluation_timeout_s([slow, slow]) == 1000.0
    assert serial_evaluation_timeout_s([fast, fast]) == Problem.evaluation_timeout_s
