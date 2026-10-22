from sregym.warm_infrastructure import marker_is_ready


def test_warm_infrastructure_requires_durable_cluster_marker() -> None:
    assert marker_is_ready("configmap/sregym-warm-infrastructure\n") is True
    assert marker_is_ready('Error from server (NotFound): configmaps "sregym-warm-infrastructure" not found') is False


def test_reused_infrastructure_still_deploys_khaos_for_a_problem_that_needs_it() -> None:
    from types import SimpleNamespace

    from sregym.conductor.conductor import Conductor, ConductorConfig

    deployed: list[str] = []
    conductor = Conductor.__new__(Conductor)
    conductor.config = ConductorConfig(preserve_infrastructure=True)
    conductor.logger = SimpleNamespace(info=lambda *a, **k: None)
    conductor._baseline_captured = True
    conductor._warm_infrastructure_ready = lambda: True
    conductor._deploy_shared_infrastructure = lambda problem: deployed.append("shared")
    conductor.khaos = SimpleNamespace(ensure_deployed=lambda: deployed.append("khaos"))
    conductor.jaeger = SimpleNamespace(create_external_name_service=lambda ns: None)
    app = SimpleNamespace(namespace="ns", name="app", deploy=lambda: None, start_workload=lambda: None)
    for needs_khaos in (True, False):
        deployed.clear()
        conductor.problem = SimpleNamespace(app=app, requires_khaos=lambda needs_khaos=needs_khaos: needs_khaos)
        conductor.deploy_app()
        assert deployed == (["khaos"] if needs_khaos else [])
