WARM_INFRASTRUCTURE_MARKER = "configmap/sregym-warm-infrastructure"


def marker_is_ready(kubectl_output: str) -> bool:
    return kubectl_output.strip() == WARM_INFRASTRUCTURE_MARKER
