import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from sregym.paths import TARGET_MICROSERVICES
from sregym.service import source_deploy


def test_docker_build_uses_configured_builder(monkeypatch) -> None:
    commands = []
    monkeypatch.setenv("SREGYM_DOCKER_BUILDER", "healthy-builder")
    monkeypatch.setattr(source_deploy.subprocess, "run", lambda command, check: commands.append(command))

    source_deploy._run_command(["docker", "build", "-t", "image", "."])

    assert commands == [["docker", "buildx", "build", "--builder", "healthy-builder", "--load", "-t", "image", "."]]


def _hotel_source(root: Path) -> Path:
    (root / "cmd").mkdir(parents=True)
    (root / "cmd" / "main.go").write_text("package main\n")
    (root / "Dockerfile").write_text("FROM golang\nCOPY . /src\n")
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (root / ".sdo").mkdir()
    (root / ".sdo" / "goal.md").write_text("keep it up\n")
    return root


def test_context_digest_ignores_repository_metadata_and_tracks_sources(tmp_path: Path) -> None:
    source = _hotel_source(tmp_path / "app")
    digest = source_deploy.source_context_digest(source)

    (source / ".git" / "HEAD").write_text("ref: refs/heads/other\n")
    (source / ".sdo" / "playbook.md").write_text("new playbook\n")
    assert source_deploy.source_context_digest(source) == digest

    (source / "cmd" / "main.go").write_text("package main // changed\n")
    assert source_deploy.source_context_digest(source) != digest


class FakeDocker:
    def __init__(self, labels: dict[str, str]) -> None:
        self.labels = labels
        self.commands: list[list[str]] = []

    def run(self, command, check=False, capture_output=False, text=False):
        self.commands.append(list(command))
        if command[:3] == ["docker", "image", "inspect"]:
            ref = command[-1]
            if ref not in self.labels:
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="No such image")
            return subprocess.CompletedProcess(command, 0, stdout=self.labels[ref] + "\n", stderr="")
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, stdout="fastloop-w0-control-plane\n", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    def kinds(self) -> list[str]:
        return [" ".join(command[:3]) for command in self.commands]


@pytest.fixture
def hotel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, FakeDocker, object]:
    source = _hotel_source(tmp_path / "hotel")
    monkeypatch.delenv("SREGYM_DOCKER_BUILDER", raising=False)
    monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
    adapter = source_deploy._HotelReservationAdapter()
    monkeypatch.setattr(adapter, "_source_dir", lambda: source)
    docker = FakeDocker({})
    monkeypatch.setattr(source_deploy.subprocess, "run", docker.run)
    return source, docker, adapter


def _image_ref() -> str:
    return f"yinfangchen/hotelreservation:{source_deploy.source_image_tag('Hotel Reservation', 'fastloop-w0')}"


def test_unchanged_build_context_skips_the_rebuild_but_still_loads_the_cluster(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    docker.labels[_image_ref()] = source_deploy.source_context_digest(source)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert docker.kinds()[0] == "docker image inspect"
    assert "kind load docker-image" in docker.kinds()


def test_unchanged_build_context_skips_the_kind_load_when_every_node_holds_the_image(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    docker.labels[_image_ref()] = source_deploy.source_context_digest(source)
    monkeypatch.setattr(source_deploy, "kind_nodes_hold_image", lambda cluster, image: True)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert "kind load docker-image" not in docker.kinds()
    assert docker.kinds()[-1] == "docker exec fastloop-w0-control-plane"


def test_changed_build_context_rebuilds_with_the_digest_label(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    docker.labels[_image_ref()] = "stale-digest"

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    build = next(command for command in docker.commands if command[:2] == ["docker", "build"])
    assert f"sregym.source-digest={source_deploy.source_context_digest(source)}" in build
    assert build[build.index("--label") + 1].startswith("sregym.source-digest=")
    assert "kind load docker-image" in docker.kinds()


def test_build_cache_is_opt_in(hotel, monkeypatch) -> None:
    _source, docker, adapter = hotel
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert docker.kinds()[:2] == ["docker build -t", "kind load docker-image"]
    assert "--label" not in docker.commands[0]


def test_kind_load_is_unconditional_without_the_build_cache(hotel, monkeypatch) -> None:
    _source, docker, adapter = hotel
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)
    monkeypatch.setattr(source_deploy, "kind_nodes_hold_image", lambda cluster, image: True)

    adapter.ensure_images_loaded(cluster_name="fastloop-w0", node_architectures=set())

    assert "kind load docker-image" in docker.kinds()


def _exec_tags(docker: FakeDocker) -> list[tuple[str, str]]:
    """(source, target) of every in-node `ctr images tag` the deploy ran."""
    return [(command[-2], command[-1]) for command in docker.commands if command[-5:-2] == ["images", "tag", "--force"]]


def test_hotel_source_build_is_published_under_the_declared_ref_and_deployed_unchanged(hotel, monkeypatch) -> None:
    source, docker, adapter = hotel
    (source / "kubernetes").mkdir()
    manifest = "image: yinfangchen/hotelreservation:latest\n"
    (source / "kubernetes" / "frontend.yaml").write_text(manifest)

    rendered_path = source / "accepted.yaml"
    rendered_path.write_text(manifest)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(rendered_path))
    monkeypatch.setattr(source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"], "_source_dir", lambda: source)
    with source_deploy.plan_for_app(SimpleNamespace(name="Hotel Reservation")) as plan:
        assert plan.manifest_path.read_text() == manifest
        assert plan.manifest_path.is_file()

    assert _exec_tags(docker) == [
        (
            "docker.io/" + _image_ref(),
            "docker.io/yinfangchen/hotelreservation:latest",
        )
    ]
    assert docker.commands[-1][:4] == ["docker", "exec", "fastloop-w0-control-plane", "ctr"]


def test_every_source_adapter_deploys_what_the_source_declares(monkeypatch, tmp_path: Path) -> None:
    """Class guard: a lane-private tag never reaches a deployed ref.

    SDO accepts the app source as the desired state and reconciles toward it, so a deploy that substitutes any
    image or tag is unexplained drift at time zero (and is reverted by the first reconcile).
    """
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
    monkeypatch.setattr(source_deploy, "_build_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(source_deploy, "_kind_load_image", lambda **kwargs: None)
    (tmp_path / "kubernetes").mkdir()
    accepted = tmp_path / "accepted.yaml"
    accepted.write_text("synthetic accepted render")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(accepted))
    tags: list[tuple[str, str]] = []
    monkeypatch.setattr(
        source_deploy, "tag_image_in_kind_nodes", lambda cluster, built, declared: tags.append((built, declared))
    )

    for app in source_deploy.all_declared_apps():
        if not source_deploy.is_supported(app):
            continue
        tags.clear()
        monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
        with source_deploy.plan_for_app(SimpleNamespace(name=app), node_architectures={"amd64"}) as plan:
            assert plan.manifest_path.is_file(), app
            assert plan.manifest_path == accepted, app
            assert plan.manifest_path.read_text() == "synthetic accepted render"
        assert tags, app
        for built, declared in tags:
            assert built.rpartition(":")[0] == declared.rpartition(":")[0], app
            assert built.rpartition(":")[2].startswith("sregym-src-"), app
            assert not declared.rpartition(":")[2].startswith("sregym-src-"), app


@pytest.mark.skipif(
    not (TARGET_MICROSERVICES / "socialNetwork" / "helm-chart").is_dir(), reason="SREGym-applications not checked out"
)
def test_declared_tags_match_the_application_sources() -> None:
    social = TARGET_MICROSERVICES / "socialNetwork" / "helm-chart" / "socialnetwork"
    values = yaml.safe_load((social / "values.yaml").read_text())
    assert values["global"]["defaultImageVersion"] == source_deploy._SOCIAL_APP_DECLARED_TAG
    for chart, expected in (
        ("nginx-thrift", source_deploy._SOCIAL_NGINX_DECLARED_TAG),
        ("media-frontend", source_deploy._SOCIAL_MEDIA_DECLARED_TAG),
    ):
        container = yaml.safe_load((social / "charts" / chart / "values.yaml").read_text())["container"]
        assert container["imageVersion"] == expected, chart
    hotel_images = {
        line.split("image:", 1)[1].strip().lstrip("- ").strip()
        for path in (TARGET_MICROSERVICES / "hotelReservation" / "kubernetes").rglob("*.yaml")
        for line in path.read_text().splitlines()
        if "image:" in line
    }
    assert f"{source_deploy._HOTEL_IMAGE_NAME}:{source_deploy._HOTEL_DECLARED_TAG}" in hotel_images
    chart = TARGET_MICROSERVICES / "astronomy-shop" / "charts" / "opentelemetry-demo"
    assert yaml.safe_load((chart / "Chart.yaml").read_text())["appVersion"] == source_deploy._ASTRONOMY_DECLARED_TAG
    assert yaml.safe_load((chart / "values.yaml").read_text())["default"]["image"]["tag"] == ""


@pytest.mark.parametrize(
    ("module", "class_name", "name", "namespace"),
    [
        ("hotel_reservation", "HotelReservation", "Hotel Reservation", "hotel-reservation"),
        ("social_network", "SocialNetwork", "Social Network", "social-network"),
        ("astronomy_shop", "AstronomyShop", "OpenTelemetry Demo Astronomy Shop", "astronomy-shop"),
    ],
)
def test_source_app_has_one_checked_manifest_writer(tmp_path, monkeypatch, module, class_name, name, namespace):
    import importlib
    import logging
    import shlex

    native = importlib.import_module(f"sregym.service.apps.{module}")
    accepted = tmp_path / "accepted render.yaml"
    accepted.write_text("synthetic accepted render")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(accepted))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "offline")
    monkeypatch.setenv("SREGYM_DEPLOY_FROM_SOURCE", "1")
    monkeypatch.setattr(source_deploy._SUPPORTED_ADAPTERS[name], "ensure_images_loaded", lambda **kw: None)
    if hasattr(native, "Helm"):
        monkeypatch.setattr(native.Helm, "assert_if_deployed", lambda ns: None)
        monkeypatch.setattr(native.Helm, "install", lambda **kw: pytest.fail("alternate Helm writer"))
    writes = []
    app = SimpleNamespace(
        name=name,
        namespace=namespace,
        logger=logging.getLogger("fixture"),
        create_namespace=lambda: None,
        helm_configs={"namespace": namespace},
        kubectl=SimpleNamespace(
            get_node_architectures=lambda: {"amd64"},
            wait_for_ready=lambda ns: None,
            create_namespace_if_not_exist=lambda ns: None,
            exec_command=lambda command: pytest.fail("unchecked manifest writer"),
            exec_command_checked=writes.append,
        ),
    )
    getattr(native, class_name).deploy(app)
    assert [shlex.split(command) for command in writes] == [["kubectl", "apply", "-f", str(accepted), "-n", namespace]]
