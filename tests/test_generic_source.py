"""Public declaration, cache, and legacy-equivalence contracts (no Docker required)."""

import hashlib
import importlib.util
import os
import sys
from pathlib import Path

import pytest
import yaml

from sregym.service import generic_source, source_deploy


def declaration(tmp_path, **updates):
    data = {
        "version": 1,
        "app": {"name": "Example", "filter": "example"},
        "source": {"path": ".", "url": "https://example.org/source.git", "ref": "a" * 40},
        "image_tag": "{tag}",
        "images": [{"name": "example/app", "context": ".", "dockerfile": "Dockerfile"}],
        "deployment": {"kind": "manifest", "path": "kubernetes", "namespace": "example"},
    }
    data.update(updates)
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    (tmp_path / "kubernetes").mkdir(exist_ok=True)
    path = tmp_path / "build.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_declaration_is_authoritative_and_supports_new_apps(tmp_path, monkeypatch):
    path = declaration(tmp_path)
    monkeypatch.setenv("SREGYM_APP_BUILD_DECLARATION", str(path))
    assert source_deploy.is_supported("Example")
    source_deploy.ensure_app_supported("Example")
    assert not source_deploy.is_supported("Unknown")
    with pytest.raises(source_deploy.SourceDeployUnsupportedError, match="Unknown"):
        source_deploy.ensure_app_supported("Unknown")
    from sregym.service.app_workspace import resolve_app_source_subdir

    assert resolve_app_source_subdir("example") == tmp_path.name


@pytest.mark.parametrize(
    "updates",
    [
        {"version": 2},
        {"images": []},
        {"image_tag": "{unknown}"},
        {"images": [{"name": "x", "context": "../escape", "dockerfile": "Dockerfile"}]},
        {"deployment": {"kind": "other", "path": ".", "namespace": "example"}},
    ],
)
def test_invalid_declarations_fail_before_build(tmp_path, updates):
    with pytest.raises((TypeError, ValueError)):
        generic_source.load_declaration(declaration(tmp_path, **updates))


def test_build_args_target_and_architecture(tmp_path):
    path = declaration(
        tmp_path,
        images=[
            {
                "name": "example/app",
                "context": ".",
                "dockerfile": "Dockerfile",
                "dockerfile_arm64": "Dockerfile.arm64",
                "build_args": {"JOBS": "1"},
                "target": "runtime",
            }
        ],
    )
    (tmp_path / "Dockerfile.arm64").write_text("FROM scratch\n")
    adapter = generic_source.GenericSourceAdapter(generic_source.load_declaration(path), source_root=tmp_path)
    (step,) = adapter.build_steps("lane", {"arm64"})
    assert step.image_ref.endswith(":sregym-src-example-lane")
    assert step.command[-1] == str(tmp_path)
    assert "JOBS=1" in step.command and "runtime" in step.command
    assert str(tmp_path / "Dockerfile.arm64") in step.command


def test_download_cache_survives_dead_origin_and_verifies_bytes(tmp_path):
    payload = b"pinned source archive"
    origin = tmp_path / "origin.tar"
    origin.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    download = generic_source.Download(url=origin.as_uri(), sha256=digest)
    cache = generic_source.DownloadCache(tmp_path / "cache")
    cached = cache.fetch(download)
    origin.unlink()
    assert cache.fetch(download).read_bytes() == payload
    cached.write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="cache|sha256"):
        cache.fetch(download)
    with pytest.raises(RuntimeError, match="cold cache"):
        generic_source.DownloadCache(tmp_path / "cold").fetch(download)


def test_download_mismatch_never_publishes(tmp_path):
    origin = tmp_path / "origin"
    origin.write_bytes(b"bad")
    cache = generic_source.DownloadCache(tmp_path / "cache")
    with pytest.raises(RuntimeError, match="sha256"):
        cache.fetch(generic_source.Download(url=origin.as_uri(), sha256="0" * 64))
    assert not list((tmp_path / "cache").glob("*.blob"))


def test_load_bounded_workers(monkeypatch):
    monkeypatch.setattr(generic_source.os, "cpu_count", lambda: 8)
    monkeypatch.setattr(generic_source.os, "getloadavg", lambda: (100.0, 0.0, 0.0))
    assert generic_source.build_workers(4) == 1
    monkeypatch.setattr(generic_source.os, "getloadavg", lambda: (0.0, 0.0, 0.0))
    assert generic_source.build_workers(2) == 2
    with pytest.raises(ValueError):
        generic_source.build_workers(0)


def legacy_module():
    # Until AS1 merges, run equivalence against its actual pinned module via this
    # optional path. Ordinary CI covers hotel/social against the installed legacy.
    path = os.environ.get("SREGYM_AS1_REFERENCE_MODULE")
    if not path:
        return source_deploy
    spec = importlib.util.spec_from_file_location("as1_reference", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    # AS1 adds this path resolver alongside the adapter; bind only its path
    # dependency while loading the unmodified adapter module.
    from sregym.service import app_workspace

    with pytest.MonkeyPatch.context() as patch:
        if not hasattr(app_workspace, "_astronomy_source_root"):
            patch.setattr(
                app_workspace, "_astronomy_source_root", lambda: Path("source_apps/astronomy_shop"), raising=False
            )
        spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "app,adapter_name",
    [
        ("hotel_reservation", "_HotelReservationAdapter"),
        ("social_network", "_SocialNetworkAdapter"),
        ("astronomy_shop", "_AstronomyShopAdapter"),
    ],
)
@pytest.mark.parametrize("architectures", [{"amd64"}, {"arm64"}])
def test_legacy_image_and_deployment_equivalence(tmp_path, monkeypatch, app, adapter_name, architectures):
    root = os.environ.get("SREGYM_BUILD_DECLARATIONS")
    if not root:
        pytest.skip("set SREGYM_BUILD_DECLARATIONS to the SDO apps directory")
    legacy = legacy_module()
    if not hasattr(legacy, adapter_name):
        pytest.skip("AS1 astronomy adapter has not landed; supply SREGYM_AS1_REFERENCE_MODULE")
    spec = generic_source.load_declaration(Path(root) / app / "build.yaml")
    adapter = generic_source.GenericSourceAdapter(spec, source_root=tmp_path)
    old = getattr(legacy, adapter_name)()
    monkeypatch.setattr(old, "_source_dir", lambda: tmp_path)
    monkeypatch.setattr(legacy, "_BUILT_IMAGES", set())
    monkeypatch.setattr(legacy, "_kind_load_image", lambda **kwargs: None)
    builds = []
    monkeypatch.setattr(legacy, "_build_image", lambda image, command, **kwargs: builds.append((image, command)))
    (tmp_path / "kubernetes").mkdir()
    from sregym import paths

    monkeypatch.setattr(paths, "TARGET_MICROSERVICES", tmp_path)
    (tmp_path / spec.deployment.path).mkdir(parents=True, exist_ok=True)
    with old.plan(cluster_name="lane", node_architectures=architectures) as old_plan:
        steps = adapter.build_steps("lane", architectures)
        assert [s.image_ref for s in steps] == [image for image, _ in builds]
        for step, (_, command) in zip(steps, builds, strict=True):
            assert step.command[-1] == command[-1]
            for option in ("--build-arg", "--target"):
                assert [step.command[i + 1] for i, v in enumerate(step.command) if v == option] == [
                    command[i + 1] for i, v in enumerate(command) if v == option
                ]
            # Hotel/social root Dockerfile is implicit in legacy Docker builds.
            expected = command[command.index("-f") + 1] if "-f" in command else str(tmp_path / "Dockerfile")
            assert step.command[step.command.index("-f") + 1] == expected
        monkeypatch.setattr(adapter, "ensure_images_loaded", lambda **kwargs: None)
        with adapter.plan(cluster_name="lane", node_architectures=architectures) as plan:
            assert plan.helm_extra_args == old_plan.helm_extra_args
            if old_plan.manifest_path:
                assert (
                    yaml.safe_load((plan.manifest_path / "kustomization.yaml").read_text())["images"]
                    == yaml.safe_load((old_plan.manifest_path / "kustomization.yaml").read_text())["images"]
                )


def test_downloads_are_real_build_inputs_and_work_offline(tmp_path, monkeypatch):
    payload = b"pinned archive"
    archive = tmp_path / "input.tar.gz"
    archive.write_bytes(payload)
    url = archive.as_uri()
    path = declaration(
        tmp_path,
        images=[
            {
                "name": "example/app",
                "context": ".",
                "dockerfile": "Dockerfile",
                "downloads": [{"url": url, "sha256": hashlib.sha256(payload).hexdigest()}],
            }
        ],
    )
    (tmp_path / "Dockerfile").write_text(f"FROM scratch\nRUN curl -fSL {url} -o archive.tar.gz\n")
    adapter = generic_source.GenericSourceAdapter(generic_source.load_declaration(path), source_root=tmp_path)
    monkeypatch.setenv("SREGYM_DOWNLOAD_CACHE", str(tmp_path / "downloads"))
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_JOBS", "1")
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)
    commands = []

    def run(command):
        commands.append(command)
        if command[:2] == ["docker", "build"]:
            generated = Path(command[command.index("-f") + 1]).read_text()
            assert url not in generated
            assert "COPY --from=sregym-downloads" in generated
            assert "cp /.sregym-downloads/" in generated
            context = Path(command[command.index("--build-context") + 1].split("=", 1)[1])
            assert any(p.read_bytes() == payload for p in context.iterdir() if p.name != "Dockerfile")

    monkeypatch.setattr(source_deploy, "_run_command", run)
    adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    archive.unlink()
    adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    assert sum(command[:3] == ["kind", "load", "docker-image"] for command in commands) == 2
    assert url in (tmp_path / "Dockerfile").read_text()


def test_build_cache_includes_dockerfile_args_and_target(tmp_path, monkeypatch):
    path = declaration(tmp_path)
    spec = generic_source.load_declaration(path)
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    monkeypatch.setenv("SREGYM_DOWNLOAD_CACHE", str(tmp_path / "cache"))
    labels = {}
    builds = []

    def run(command):
        if command[:2] == ["docker", "build"]:
            builds.append(command)
            labels[command[command.index("-t") + 1]] = command[command.index("--label") + 1].split("=", 1)[1]

    monkeypatch.setattr(source_deploy, "_run_command", run)
    monkeypatch.setattr(source_deploy, "_local_image_digest_label", labels.get)
    monkeypatch.setattr(source_deploy, "kind_nodes_hold_image", lambda *args: True)
    adapter = generic_source.GenericSourceAdapter(spec)
    adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    assert len(builds) == 1
    from dataclasses import replace

    changed = replace(spec, images=(replace(spec.images[0], build_args={"VERSION": "new"}, target="runtime"),))
    generic_source.GenericSourceAdapter(changed).ensure_images_loaded(cluster_name="lane", node_architectures=set())
    assert len(builds) == 2


def test_missing_or_escaping_inputs_never_build(tmp_path, monkeypatch):
    path = declaration(tmp_path)
    (tmp_path / "Dockerfile").unlink()
    monkeypatch.setattr(source_deploy, "_run_command", lambda command: pytest.fail("must validate first"))
    adapter = generic_source.GenericSourceAdapter(generic_source.load_declaration(path))
    with pytest.raises(ValueError, match="missing"):
        adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    outside = tmp_path.parent / "outside-Dockerfile"
    outside.write_text("FROM scratch\n")
    (tmp_path / "Dockerfile").symlink_to(outside)
    with pytest.raises(ValueError, match="escapes"):
        adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())


def test_declaration_drives_workspace_and_path_resolution(tmp_path, monkeypatch):
    from sregym.service import app_workspace

    source = tmp_path / "source"
    source.mkdir()
    path = declaration(source)
    (source / "data.txt").write_text("input")
    monkeypatch.setenv("SREGYM_APP_BUILD_DECLARATION", str(path))
    monkeypatch.delenv("SREGYM_APP_SOURCE_DIR", raising=False)
    assert "Example" in source_deploy.all_declared_apps()
    assert app_workspace.resolve_app_relative_path("example", "source/data.txt") == source / "data.txt"
    assert app_workspace.resolve_workspace_path("source/data.txt") == source / "data.txt"
    workspace = app_workspace.prepare_application_workspace(
        experiment_dir=tmp_path / "experiment", app_filter="example", resume=False
    )
    assert (workspace / "data.txt").read_text() == "input"
    assert (workspace / ".git").is_dir()
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(workspace))
    assert app_workspace.resolve_workspace_path("source/data.txt") == workspace / "data.txt"
    assert app_workspace.resolve_app_relative_path("example", "data.txt") == workspace / "data.txt"
    with pytest.raises(ValueError, match="does not match"):
        app_workspace.resolve_app_source_subdir("different")


def test_bounded_builds_load_every_image_and_propagate_failure(tmp_path, monkeypatch):
    import threading
    import time

    path = declaration(
        tmp_path, images=[{"name": f"example/app{i}", "context": ".", "dockerfile": "Dockerfile"} for i in range(5)]
    )
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_JOBS", "2")
    monkeypatch.setenv("SREGYM_DOWNLOAD_CACHE", str(tmp_path / "cache"))
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)
    monkeypatch.setattr(generic_source.os, "getloadavg", lambda: (0.0, 0.0, 0.0))
    monkeypatch.setattr(generic_source.os, "cpu_count", lambda: 8)
    lock = threading.Lock()
    active = 0
    peak = 0
    loaded = []

    def run(command):
        nonlocal active, peak
        if command[:2] == ["docker", "build"]:
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1
        else:
            loaded.append(command[3])

    monkeypatch.setattr(source_deploy, "_run_command", run)
    adapter = generic_source.GenericSourceAdapter(generic_source.load_declaration(path))
    adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())
    assert 1 <= peak <= 2
    assert len(loaded) == 5

    def fail(command):
        raise RuntimeError("build failed")

    monkeypatch.setattr(source_deploy, "_run_command", fail)
    with pytest.raises(RuntimeError, match="build failed"):
        adapter.ensure_images_loaded(cluster_name="lane", node_architectures=set())


def test_manifest_file_is_a_declaration_source(tmp_path, monkeypatch):
    path = declaration(tmp_path, deployment={"kind": "manifest", "path": "app.yaml", "namespace": "example"})
    (tmp_path / "app.yaml").write_text("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: synthetic\n")
    adapter = generic_source.GenericSourceAdapter(generic_source.load_declaration(path))
    monkeypatch.setattr(adapter, "ensure_images_loaded", lambda **kwargs: None)
    with adapter.plan(cluster_name="lane", node_architectures=set()) as plan:
        assert (plan.manifest_path.parent / "base" / "app.yaml").is_file()
        assert plan.namespace == "example"


def test_plan_for_app_routes_declaration_and_rejects_namespace_mismatch(tmp_path, monkeypatch):
    from types import SimpleNamespace

    path = declaration(tmp_path)
    monkeypatch.setenv("SREGYM_APP_BUILD_DECLARATION", str(path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "lane")
    monkeypatch.setattr(generic_source.GenericSourceAdapter, "ensure_images_loaded", lambda *args, **kwargs: None)
    with source_deploy.plan_for_app(SimpleNamespace(name="Example", namespace="example")) as plan:
        assert plan.namespace == "example"
        assert (plan.manifest_path / "kustomization.yaml").is_file()
    with pytest.raises(ValueError, match="namespace"):
        with source_deploy.plan_for_app(SimpleNamespace(name="Example", namespace="wrong")):
            pytest.fail("mismatched namespace must fail")
