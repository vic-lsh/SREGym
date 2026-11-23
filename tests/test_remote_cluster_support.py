"""Running SREGym against a remote (EKS-like) cluster: kind-only steps are skipped, images come from a registry."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from sregym.service import cluster_platform, image_mirror, k8s_proxy, source_deploy

ECR = "000000000000.dkr.ecr.us-west-2.amazonaws.com"  # synthetic account


# ---------------------------------------------------------------- platform


@pytest.mark.parametrize(
    ("explicit", "cluster", "expected"),
    [
        ("", "", "kind"),
        ("", "fastloop-w0", "kind"),
        ("", "remote-0123456789ab", "remote"),
        ("eks", "", "eks"),
        ("kind", "remote-0123456789ab", "kind"),
    ],
)
def test_cluster_platform(monkeypatch, explicit: str, cluster: str, expected: str) -> None:
    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", explicit)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", cluster)
    assert cluster_platform.cluster_platform() == expected


def test_cluster_platform_rejects_unknown_value(monkeypatch) -> None:
    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", "gke-ish")
    with pytest.raises(ValueError, match="SREGYM_CLUSTER_PLATFORM"):
        cluster_platform.cluster_platform()


# ---------------------------------------------------------------- image mirror


@pytest.mark.parametrize(
    ("ref", "canonical"),
    [
        ("mongo:4.4.6", "docker.io/library/mongo:4.4.6"),
        ("memcached", "docker.io/library/memcached:latest"),
        ("yinfangchen/hotelreservation:latest", "docker.io/yinfangchen/hotelreservation:latest"),
        ("docker.io/grafana/loki:3.6.11", "docker.io/grafana/loki:3.6.11"),
        ("quay.io/prometheus/prometheus:v2.47.2", "quay.io/prometheus/prometheus:v2.47.2"),
        ("localhost:5000/x/y", "localhost:5000/x/y:latest"),
        ("busybox@sha256:" + "a" * 64, "docker.io/library/busybox@sha256:" + "a" * 64),
    ],
)
def test_parse_image_normalizes_like_containerd(ref: str, canonical: str) -> None:
    assert image_mirror.parse_image(ref).canonical == canonical


def test_rewrite_is_off_without_configuration() -> None:
    assert image_mirror.rewrite_image("mongo:4.4.6", {}) == "mongo:4.4.6"
    text = "kind: Pod\nspec:\n  containers:\n  - image: mongo:4.4.6\n"
    assert image_mirror.rewrite_manifest_text(text, {}) == text


def test_rewrite_prefix_and_per_registry_layouts() -> None:
    env = {
        "SREGYM_IMAGE_MIRROR_PREFIX": f"{ECR}/sregym-mirror/",
        "SREGYM_IMAGE_MIRRORS": f"quay.io={ECR}/quay",
    }
    assert image_mirror.rewrite_image("mongo:4.4.6", env) == f"{ECR}/sregym-mirror/docker.io/library/mongo:4.4.6"
    assert image_mirror.rewrite_image("quay.io/prometheus/prometheus:v2.47.2", env) == (
        f"{ECR}/quay/prometheus/prometheus:v2.47.2"
    )
    # Never rewritten: already in ECR, already mirrored.
    eks = "111111111111.dkr.ecr.us-west-2.amazonaws.com/eks/kube-proxy:v1.31.0"
    assert image_mirror.rewrite_image(eks, env) == eks
    mirrored = f"{ECR}/sregym-mirror/docker.io/library/mongo:4.4.6"
    assert image_mirror.rewrite_image(mirrored, env) == mirrored


def test_rewrite_manifest_covers_containers_init_containers_and_image_env() -> None:
    env = {"SREGYM_IMAGE_MIRROR_PREFIX": f"{ECR}/m"}
    manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "spec": {
            "template": {
                "spec": {
                    "initContainers": [{"name": "init", "image": "busybox:1.36"}],
                    "containers": [
                        {
                            "name": "app",
                            "image": "openebs/provisioner-localpv:3.4.0",
                            "env": [
                                {"name": "OPENEBS_IO_HELPER_IMAGE", "value": "openebs/linux-utils:3.4.0"},
                                {"name": "OTHER", "value": "mongo:4.4.6"},
                            ],
                        }
                    ],
                }
            }
        },
    }
    text = yaml.safe_dump(manifest) + "---\n" + yaml.safe_dump({"kind": "Service", "metadata": {"name": "s"}})
    documents = list(yaml.safe_load_all(image_mirror.rewrite_manifest_text(text, env)))
    spec = documents[0]["spec"]["template"]["spec"]
    assert spec["initContainers"][0]["image"] == f"{ECR}/m/docker.io/library/busybox:1.36"
    assert spec["containers"][0]["image"] == f"{ECR}/m/docker.io/openebs/provisioner-localpv:3.4.0"
    assert spec["containers"][0]["env"][0]["value"] == f"{ECR}/m/docker.io/openebs/linux-utils:3.4.0"
    assert spec["containers"][0]["env"][1]["value"] == "mongo:4.4.6"
    assert documents[1]["kind"] == "Service"


def test_helm_post_renderer_rewrites_stdin(monkeypatch) -> None:
    import subprocess

    monkeypatch.setenv("SREGYM_IMAGE_MIRROR_PREFIX", f"{ECR}/m")
    args = image_mirror.helm_post_renderer_args()
    assert args[0] == "--post-renderer"
    command = [args[1], *(args[i + 1] for i in range(2, len(args), 2))]
    completed = subprocess.run(
        command, input="kind: Pod\nspec:\n  containers:\n  - image: mongo:4.4.6\n", capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr
    assert f"{ECR}/m/docker.io/library/mongo:4.4.6" in completed.stdout


def test_no_post_renderer_without_mirror(monkeypatch) -> None:
    monkeypatch.delenv("SREGYM_IMAGE_MIRROR_PREFIX", raising=False)
    monkeypatch.delenv("SREGYM_IMAGE_MIRRORS", raising=False)
    assert image_mirror.helm_post_renderer_args() == []


# ---------------------------------------------------------------- registry delivery


def test_remote_cluster_deploys_hotel_from_the_declared_manifests_without_kind(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "hotelReservation"
    (source / "kubernetes").mkdir(parents=True)
    (source / "kubernetes" / "kustomization.yaml").write_text("resources:\n- frontend.yaml\n")
    (source / "kubernetes" / "frontend.yaml").write_text(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: frontend\nspec:\n  template:\n    spec:\n"
        "      containers:\n      - name: f\n        image: yinfangchen/hotelreservation:latest\n"
    )
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "remote-0123456789ab")
    monkeypatch.delenv("SREGYM_CLUSTER_PLATFORM", raising=False)
    monkeypatch.delenv("SREGYM_IMAGE_DELIVERY", raising=False)
    monkeypatch.delenv("SREGYM_ACCEPTED_RENDER", raising=False)
    monkeypatch.setenv("SREGYM_IMAGE_MIRROR_PREFIX", f"{ECR}/m")
    adapter = source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"]
    monkeypatch.setattr(adapter, "_source_dir", lambda: source)

    def forbidden(*args, **kwargs):
        raise AssertionError("registry delivery must not build or kind-load")

    monkeypatch.setattr(source_deploy, "_build_image", forbidden)
    monkeypatch.setattr(source_deploy, "_kind_load_image", forbidden)
    monkeypatch.setattr(source_deploy, "tag_image_in_kind_nodes", forbidden)

    assert source_deploy.image_delivery() == "registry"
    with source_deploy.plan_for_app(SimpleNamespace(name="Hotel Reservation")) as plan:
        rendered = list(yaml.safe_load_all(plan.manifest_path.read_text()))
    image = rendered[0]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image == f"{ECR}/m/docker.io/yinfangchen/hotelreservation:latest"


def test_registry_delivery_prefers_the_accepted_render(monkeypatch, tmp_path: Path) -> None:
    accepted = tmp_path / "accepted.json"
    accepted.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": []}))
    monkeypatch.setenv("SREGYM_IMAGE_DELIVERY", "registry")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(accepted))
    monkeypatch.delenv("SREGYM_IMAGE_MIRROR_PREFIX", raising=False)
    monkeypatch.delenv("SREGYM_IMAGE_MIRRORS", raising=False)
    with source_deploy.plan_for_app(SimpleNamespace(name="Social Network")) as plan:
        assert json.loads(plan.manifest_path.read_text())["kind"] == "List"


def test_kind_lanes_keep_kind_delivery(monkeypatch) -> None:
    monkeypatch.delenv("SREGYM_IMAGE_DELIVERY", raising=False)
    monkeypatch.delenv("SREGYM_CLUSTER_PLATFORM", raising=False)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    assert source_deploy.image_delivery() == "build"


def test_remote_cluster_builds_when_the_harness_asks(monkeypatch) -> None:
    """A harness that replaces the load hooks (an EKS lane pushing to a node-mirrored registry) builds from source;
    ``kind`` stays an accepted name for the build path."""
    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", "eks")
    for value in ("build", "kind"):
        monkeypatch.setenv("SREGYM_IMAGE_DELIVERY", value)
        assert source_deploy.image_delivery() == "build"
    monkeypatch.setenv("SREGYM_IMAGE_DELIVERY", "push")
    with pytest.raises(ValueError, match="SREGYM_IMAGE_DELIVERY"):
        source_deploy.image_delivery()


# ---------------------------------------------------------------- proxy upstream auth


def _kubeconfig(tmp_path: Path, user: dict) -> Path:
    path = tmp_path / "kubeconfig"
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "c",
                "contexts": [{"name": "c", "context": {"cluster": "k", "user": "u"}}],
                "clusters": [{"name": "k", "cluster": {"server": "https://example.invalid"}}],
                "users": [{"name": "u", "user": user}],
            }
        )
    )
    return path


def test_proxy_uses_a_static_kubeconfig_token(tmp_path: Path) -> None:
    source = k8s_proxy._UpstreamToken.from_kubeconfig(str(_kubeconfig(tmp_path, {"token": "synthetic-token"})))
    assert source is not None and source.get() == "synthetic-token"


def test_proxy_reads_token_file_relative_to_kubeconfig(tmp_path: Path) -> None:
    (tmp_path / "tok").write_text("from-file\n")
    source = k8s_proxy._UpstreamToken.from_kubeconfig(str(_kubeconfig(tmp_path, {"tokenFile": "tok"})))
    assert source is not None and source.get() == "from-file"


def test_proxy_runs_and_caches_an_exec_plugin(tmp_path: Path) -> None:
    counter = tmp_path / "calls"
    script = tmp_path / "plugin.py"
    script.write_text(
        "import json, pathlib\n"
        f"p = pathlib.Path({str(counter)!r}); n = int(p.read_text()) + 1 if p.exists() else 1; p.write_text(str(n))\n"
        "print(json.dumps({'apiVersion': 'client.authentication.k8s.io/v1beta1', 'kind': 'ExecCredential',"
        " 'status': {'token': f'exec-{n}', 'expirationTimestamp': '2999-01-01T00:00:00Z'}}))\n"
    )
    user = {
        "exec": {
            "apiVersion": "client.authentication.k8s.io/v1beta1",
            "command": sys.executable,
            "args": [str(script)],
            "interactiveMode": "Never",
        }
    }
    source = k8s_proxy._UpstreamToken.from_kubeconfig(str(_kubeconfig(tmp_path, user)))
    assert source is not None
    assert source.get() == "exec-1"
    assert source.get() == "exec-1"  # cached until a minute before expiry
    source._expires_at = 0
    assert source.get() == "exec-2"


def test_client_certificate_kubeconfig_has_no_token_source(tmp_path: Path) -> None:
    path = _kubeconfig(tmp_path, {"client-certificate-data": "eA==", "client-key-data": "eA=="})
    assert k8s_proxy._UpstreamToken.from_kubeconfig(str(path)) is None


def test_agent_kubeconfig_check_skips_kind_node_names_on_remote_clusters(monkeypatch, tmp_path: Path) -> None:
    """EKS nodes are named ip-..., never <cluster>-...: the node-prefix proof applies to kind only."""
    from sregym.conductor.conductor import Conductor

    calls: list[dict] = []
    monkeypatch.setattr(
        "sregym.conductor.conductor.verify_agent_kubeconfig", lambda path, **kwargs: calls.append(kwargs)
    )
    conductor = Conductor.__new__(Conductor)
    conductor._agent_kubeconfig_path = str(tmp_path / "agent")
    conductor.k8s_proxy = SimpleNamespace(listen_port=16443)
    monkeypatch.setattr("sregym.service.k8s_proxy.KubernetesAPIProxy._INCLUSTER_TOKEN_PATH", str(tmp_path / "no-token"))
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "remote.kubeconfig"))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "remote-0123456789ab")
    monkeypatch.delenv("SREGYM_CLUSTER_PLATFORM", raising=False)
    conductor._verify_agent_kubeconfig()
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    conductor._verify_agent_kubeconfig()
    assert [call["cluster_name"] for call in calls] == [None, "fastloop-w0"]
    # A remote cluster is proved by its identity through the proxy's upstream kubeconfig instead.
    assert [call["reference_kubeconfig"] for call in calls] == [str(tmp_path / "remote.kubeconfig"), None]


# ---------------------------------------------------------------- conductor cleanup


def test_fix_kubernetes_never_resets_managed_kube_proxy_or_touches_calico(monkeypatch) -> None:
    from sregym.conductor import conductor as conductor_module

    replaced: list[str] = []
    calico: list[str] = []

    class FakeVirtual:
        def __init__(self, namespace: str) -> None:
            pass

        def recover_daemon_set_image_replacement(self, daemon_set_name: str, original_image: str) -> None:
            replaced.append(daemon_set_name)

        def recover_all_nxdomain_templates(self) -> None:
            pass

    class FakeRemoteOS:
        def recover_kubelet_crash(self) -> None:
            pass

        def recover_disk_pressure_all(self) -> None:
            pass

        def recover_clock_drift(self) -> None:
            pass

    monkeypatch.setattr(conductor_module, "VirtualizationFaultInjector", FakeVirtual)
    monkeypatch.setattr(conductor_module, "RemoteOSFaultInjector", FakeRemoteOS)
    conductor = conductor_module.Conductor.__new__(conductor_module.Conductor)
    conductor.logger = conductor_module.logging.getLogger("test")
    conductor.kubectl = SimpleNamespace(exec_command=lambda *a, **k: "")
    conductor.dm_flakey_manager = SimpleNamespace(teardown_openebs_dm_flakey_infrastructure=lambda: None)
    monkeypatch.setattr(conductor, "_fix_calico_ippool", lambda: calico.append("ippool"), raising=False)
    monkeypatch.setattr(
        conductor, "_fix_calico_route_reflector_label_drift", lambda: calico.append("rr"), raising=False
    )

    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", "eks")
    conductor.fix_kubernetes()
    assert replaced == [] and calico == []

    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", "kind")
    conductor.fix_kubernetes()
    assert replaced == ["kube-proxy"] and calico == ["ippool", "rr"]
