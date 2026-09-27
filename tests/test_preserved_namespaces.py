from types import SimpleNamespace

from sregym.service.cluster_state import (
    PRESERVE_NAMESPACE_LABEL_ENV,
    preserved_namespace_names,
    reconcilable_namespaces,
    reconcilable_persistent_volumes,
)


class _CoreV1:
    def __init__(self, labelled: dict[str, dict[str, str]]) -> None:
        self.labelled = labelled
        self.selectors: list[str] = []

    def list_namespace(self, label_selector: str = ""):
        self.selectors.append(label_selector)
        key, _, value = label_selector.partition("=")
        items = [
            SimpleNamespace(metadata=SimpleNamespace(name=name))
            for name, labels in self.labelled.items()
            if labels.get(key) == value
        ]
        return SimpleNamespace(items=items)


def _pv(name: str, claim_namespace: str | None):
    claim = None if claim_namespace is None else SimpleNamespace(namespace=claim_namespace)
    return SimpleNamespace(metadata=SimpleNamespace(name=name), spec=SimpleNamespace(claim_ref=claim))


def test_namespaces_are_only_preserved_when_the_runner_opts_in(monkeypatch) -> None:
    core_v1 = _CoreV1({"app-sdo": {"sdo.dev/preserve": "true"}, "other": {}})
    monkeypatch.delenv(PRESERVE_NAMESPACE_LABEL_ENV, raising=False)
    assert preserved_namespace_names(core_v1) == set()
    assert core_v1.selectors == []

    monkeypatch.setenv(PRESERVE_NAMESPACE_LABEL_ENV, "sdo.dev/preserve")
    assert preserved_namespace_names(core_v1) == {"app-sdo"}
    assert core_v1.selectors == ["sdo.dev/preserve=true"]


def test_reconcile_keeps_preserved_namespaces_and_their_volumes() -> None:
    current = {"default", "app", "app-sdo", "leftover"}
    assert reconcilable_namespaces(current, baseline={"default"}, preserved={"app-sdo"}) == {"app", "leftover"}

    volumes = [_pv("pv-app", "app"), _pv("pv-sdo", "app-sdo"), _pv("pv-free", None), _pv("pv-base", "observe")]
    assert reconcilable_persistent_volumes(volumes, baseline={"pv-base"}, preserved={"app-sdo"}) == {
        "pv-app",
        "pv-free",
    }
