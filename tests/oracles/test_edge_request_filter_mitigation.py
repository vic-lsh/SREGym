"""EdgeRequestFilterMitigationOracle: the bad WAF regex is inactive and frontend pods are ready."""

from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.edge_request_filter_mitigation import EdgeRequestFilterMitigationOracle

BAD = "^(a+)+$"


def _env(**values):
    return [SimpleNamespace(name=k, value=v) for k, v in values.items()]


def _pod(name, phase="Running", ready=True, component="frontend"):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels={"app.kubernetes.io/component": component}),
        status=SimpleNamespace(phase=phase, container_statuses=[SimpleNamespace(name="frontend", ready=ready)]),
    )


class _Kubectl:
    def __init__(self, env, pods):
        self.env, self.pods = env, pods

    def get_deployment(self, name, namespace):
        container = SimpleNamespace(name="frontend", env=self.env)
        return SimpleNamespace(
            spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[container])))
        )

    def list_deployments(self, namespace):  # MitigationOracle records replica counts at construction
        return SimpleNamespace(items=[])

    def list_pods(self, namespace):
        return SimpleNamespace(items=self.pods)


def _evaluate(env, pods=None):
    pods = [_pod("frontend-1"), _pod("ad-1", component="ad")] if pods is None else pods
    problem = SimpleNamespace(
        kubectl=_Kubectl(env, pods), namespace="astronomy-shop", faulty_service="frontend", bad_regex=BAD
    )
    oracle = EdgeRequestFilterMitigationOracle(problem)
    oracle.namespace_settled = True
    return oracle.evaluate()["success"]


def test_active_bad_regex_fails():
    assert _evaluate(_env(WAF_RULE_REGEX=BAD)) is False
    assert _evaluate(_env(WAF_RULE_REGEX=BAD, WAF_RULE_ENABLED="true")) is False


@pytest.mark.parametrize(
    "env",
    [
        _env(WAF_RULE_REGEX=BAD, WAF_RULE_ENABLED="False"),  # case-insensitive disable
        _env(WAF_RULE_REGEX="^a+$"),  # replaced
        [],  # rolled back
    ],
)
def test_disabled_replaced_or_rolled_back_rule_passes_with_ready_pods(env):
    assert _evaluate(env) is True


@pytest.mark.parametrize(
    "pods",
    [[], [_pod("frontend-1", phase="Pending")], [_pod("frontend-1"), _pod("frontend-2", ready=False)]],
)
def test_missing_or_unready_frontend_pods_fail(pods):
    assert _evaluate([], pods) is False
