"""``KubeCtl()`` reports why the kubeconfig could not be loaded.

Regression: any error from ``load_kube_config`` (here a deleted temp directory,
which the kubernetes package needs for embedded certificate data) became the
log line "Missing kubeconfig" and ``exit(1)``. The injection worker died in the
middle of a composite injection with the cause lost and the faults already
injected left behind.
"""

import tempfile

import pytest

kubectl_mod = pytest.importorskip("sregym.service.kubectl")

_CERT = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg=="
_KUBECONFIG = f"""
apiVersion: v1
kind: Config
clusters: [{{name: c, cluster: {{server: "https://127.0.0.1:1", certificate-authority-data: {_CERT}}}}}]
users: [{{name: u, user: {{client-certificate-data: {_CERT}, client-key-data: {_CERT}}}}}]
contexts: [{{name: x, context: {{cluster: c, user: u}}}}]
current-context: x
"""


def test_a_kubeconfig_load_failure_raises_with_the_real_cause(tmp_path, monkeypatch):
    config_file = tmp_path / "kubeconfig"
    config_file.write_text(_KUBECONFIG)
    gone = tmp_path / "gone"
    gone.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(gone))
    gone.rmdir()
    monkeypatch.setenv("KUBECONFIG", str(config_file))

    with pytest.raises(kubectl_mod.KubeConfigError) as raised:
        kubectl_mod.KubeCtl()

    assert str(config_file) in str(raised.value)
    assert "FileNotFoundError" in str(raised.value)
    assert isinstance(raised.value.__cause__, FileNotFoundError)


def test_a_missing_kubeconfig_file_is_named_in_the_error(tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "absent"))
    with pytest.raises(kubectl_mod.KubeConfigError, match="absent"):
        kubectl_mod.KubeCtl()


def test_the_fastloop_worker_survives_a_kubeconfig_failure_and_reports_it(tmp_path, monkeypatch):
    """The worker keeps the injected faults' state, so the driver's next ``recover`` can revert them."""
    import io
    import json

    worker_mod = pytest.importorskip("sregym.fastloop.worker")
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "absent"))

    class Worker:
        def handle(self, request):
            if request["op"] == "inject":
                kubectl_mod.KubeCtl()
            return {"op": request["op"]}

    requests = io.StringIO("".join(json.dumps(r) + "\n" for r in ({"id": 1, "op": "inject"}, {"id": 2, "op": "recover"})))
    replies = io.StringIO()
    worker_mod.serve(Worker(), requests, replies)

    first, second = (json.loads(line) for line in replies.getvalue().splitlines())
    assert first["ok"] is False
    assert "KubeConfigError" in first["error"]
    assert "absent" in first["error"]
    assert second == {"id": 2, "ok": True, "result": {"op": "recover"}}
