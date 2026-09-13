from sregym.warm_infrastructure import marker_is_ready


def test_warm_infrastructure_requires_durable_cluster_marker() -> None:
    assert marker_is_ready("configmap/sregym-warm-infrastructure\n") is True
    assert marker_is_ready('Error from server (NotFound): configmaps "sregym-warm-infrastructure" not found') is False
