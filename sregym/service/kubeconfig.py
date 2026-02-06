import os


def require_kubeconfig_path(
    kubeconfig_path: str | None = None,
    *,
    env_keys: tuple[str, ...] = ("SREGYM_BASE_KUBECONFIG", "KUBECONFIG"),
) -> str:
    """Resolve kubeconfig path from explicit input or required environment variables.

    This function intentionally does NOT fall back to ~/.kube/config to avoid
    cross-worker leakage and implicit host-level state.
    """
    if kubeconfig_path:
        return kubeconfig_path.split(os.pathsep)[0]

    for key in env_keys:
        value = os.getenv(key)
        if value:
            return value.split(os.pathsep)[0]

    keys = ", ".join(env_keys)
    raise RuntimeError(
        f"No explicit kubeconfig provided. Set one of [{keys}] "
        "or pass kubeconfig_path explicitly."
    )

