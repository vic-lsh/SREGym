from types import SimpleNamespace

from sregym.generators.fault.inject_app import ApplicationFaultInjector


class _DeploymentKubeCtl:
    def __init__(self, env):
        self.env = dict(env)

    def get_deployment(self, name, namespace):
        env = [SimpleNamespace(name=k, value=v) for k, v in self.env.items()]
        container = SimpleNamespace(name="checkout", env=env)
        return SimpleNamespace(
            spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[container])))
        )

    def patch_deployment(self, name, namespace, body):
        for var in body["spec"]["template"]["spec"]["containers"][0]["env"]:
            self.env[var["name"]] = var["value"]


def _injector(env):
    injector = object.__new__(ApplicationFaultInjector)
    injector.namespace = "astronomy-shop"
    injector.kubectl = _DeploymentKubeCtl(env)
    return injector


def test_scheme_prefixed_address_keeps_its_host_through_injection_and_recovery():
    injector = _injector({"SHIPPING_ADDR": "http://shipping:8080", "OTHER": "x"})

    injector.inject_incorrect_port_assignment("checkout", "checkout", "SHIPPING_ADDR", incorrect_port="8082")
    assert injector.kubectl.env["SHIPPING_ADDR"] == "http://shipping:8082"

    injector.recover_incorrect_port_assignment("checkout", "SHIPPING_ADDR", correct_port="8080")
    assert injector.kubectl.env == {"SHIPPING_ADDR": "http://shipping:8080", "OTHER": "x"}


def test_plain_host_port_address_is_unchanged_in_form():
    injector = _injector({"PRODUCT_CATALOG_ADDR": "product-catalog:8080"})

    injector.inject_incorrect_port_assignment("checkout", "checkout", "PRODUCT_CATALOG_ADDR", incorrect_port="8082")
    assert injector.kubectl.env["PRODUCT_CATALOG_ADDR"] == "product-catalog:8082"

    injector.recover_incorrect_port_assignment("checkout", "PRODUCT_CATALOG_ADDR")
    assert injector.kubectl.env["PRODUCT_CATALOG_ADDR"] == "product-catalog:8080"
