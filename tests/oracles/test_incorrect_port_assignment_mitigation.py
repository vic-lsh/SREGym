from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.incorrect_port import IncorrectPortAssignmentMitigationOracle


def _oracle(env):
    container = SimpleNamespace(name="checkout", env=env)
    deployment = SimpleNamespace(
        spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[container])))
    )
    problem = SimpleNamespace(
        namespace="astronomy-shop",
        faulty_service="checkout",
        env_var="PRODUCT_CATALOG_ADDR",
        correct_port="8080",
        kubectl=SimpleNamespace(get_deployment=lambda name, namespace: deployment),
    )
    return IncorrectPortAssignmentMitigationOracle(problem)


def _env(value):
    return [SimpleNamespace(name="OTHER", value="x:8082"), SimpleNamespace(name="PRODUCT_CATALOG_ADDR", value=value)]


@pytest.mark.parametrize(
    "address",
    ["product-catalog:8080", "product-catalog.astronomy-shop.svc.cluster.local:8080", "http://product-catalog:8080"],
)
def test_passes_when_the_trailing_port_is_the_correct_port(address):
    assert _oracle(_env(address)).evaluate()["success"] is True


@pytest.mark.parametrize("address", ["product-catalog:8082", "product-catalog", "http://product-catalog:9090"])
def test_fails_when_the_trailing_port_is_wrong_or_missing(address):
    assert _oracle(_env(address)).evaluate()["success"] is False


@pytest.mark.parametrize("env", [None, [SimpleNamespace(name="OTHER", value="x:8080")]])
def test_fails_when_the_env_var_is_absent(env):
    assert _oracle(env).evaluate()["success"] is False
