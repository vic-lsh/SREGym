"""Astronomy source/workspace contracts without Docker or Kubernetes."""

from types import SimpleNamespace

from sregym.service import app_workspace, source_deploy

APP = "OpenTelemetry Demo Astronomy Shop"


def test_astronomy_workspace_copies_pinned_source_not_chart(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "src").mkdir()
    (source / "src/app.py").write_text("print('source')\n")
    monkeypatch.setattr(app_workspace, "_astronomy_source_root", lambda: source)
    workspace = app_workspace.prepare_application_workspace(
        experiment_dir=tmp_path / "experiment", app_filter="astronomy_shop", resume=False
    )
    assert (workspace / "src/app.py").read_text() == "print('source')\n"
    assert (workspace / ".git").is_dir()
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(workspace))
    assert source_deploy._resolve_app_source_dir(APP, source) == workspace
    # Chart inputs remain in the pinned chart checkout, separate from demo source.
    assert (
        app_workspace.resolve_workspace_path("astronomy-shop/charts/opentelemetry-demo")
        == app_workspace._target_microservices_root() / "astronomy-shop/charts/opentelemetry-demo"
    )


def test_astronomy_plan_builds_every_demo_component_and_preserves_chart(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(source))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-pilot-52")
    builds, loads = [], []
    monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
    monkeypatch.setattr(
        source_deploy, "_build_image", lambda ref, cmd, *, context_dir: builds.append((ref, cmd, context_dir))
    )
    monkeypatch.setattr(source_deploy, "_kind_load_image", lambda **kw: loads.append(kw))
    with source_deploy.plan_for_app(SimpleNamespace(name=APP), node_architectures={"amd64"}) as plan:
        args = plan.helm_extra_args
        assert plan.manifest_path is None
        assert len(builds) == len(loads) == 20
        assert len({ref for ref, _, _ in builds}) == 20
        for ref, command, context in builds:
            assert context == source
            assert str(source) == command[-1]
            assert "--build-arg" in command or ref.endswith(("-ad", "-currency", "-fraud-detection", "-kafka")) is False
        rendered_values = dict(arg.split("=", 1) for arg in args if "=" in arg)
        assert rendered_values["default.image.tag"].startswith("sregym-src-")
        assert len(rendered_values) == 1
        assert not any("enabled=" in arg or "envOverrides" in arg for arg in args)
        assert not any("valkey" in ref or "postgres" in ref or "grafana" in ref for ref, _, _ in builds)
    with source_deploy.plan_for_app(SimpleNamespace(name=APP), node_architectures={"amd64"}):
        assert len(builds) == 20


def test_astronomy_custom_cart_dockerfile_and_build_arguments(monkeypatch, tmp_path):
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(tmp_path))
    monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
    monkeypatch.setattr(source_deploy, "_kind_load_image", lambda **kw: None)
    builds = []
    monkeypatch.setattr(source_deploy, "_build_image", lambda ref, cmd, **kw: builds.append((ref, cmd)))
    source_deploy._AstronomyShopAdapter().ensure_images_loaded(cluster_name="lane", node_architectures={"amd64"})
    for ref, command in builds:
        if ref.endswith("-cart"):
            assert str(tmp_path / "src/cart/src/Dockerfile") in command
        if ref.endswith(("-ad", "-fraud-detection", "-kafka")):
            assert "OTEL_JAVA_AGENT_VERSION=2.23.0" in command
        if ref.endswith("-currency"):
            assert "OPENTELEMETRY_CPP_VERSION=1.24.0" in command


def test_astronomy_workspace_preserves_audit_but_allows_historical_advisories(tmp_path, monkeypatch):
    source = tmp_path / "source"
    for component in ("accounting", "cart"):
        path = source / "src" / component / "Directory.Build.props"
        path.parent.mkdir(parents=True)
        path.write_text(
            "<Project><PropertyGroup><NuGetAudit>true</NuGetAudit><TreatWarningsAsErrors>true</TreatWarningsAsErrors></PropertyGroup></Project>"
        )
    monkeypatch.setattr(app_workspace, "_astronomy_source_root", lambda: source)
    workspace = app_workspace.prepare_application_workspace(
        experiment_dir=tmp_path / "experiment", app_filter="astronomy_shop", resume=False
    )
    for component in ("accounting", "cart"):
        content = (workspace / "src" / component / "Directory.Build.props").read_text()
        assert "<NuGetAudit>true</NuGetAudit>" in content
        assert "<TreatWarningsAsErrors>true</TreatWarningsAsErrors>" in content
        assert "NU1901;NU1902;NU1903;NU1904" in content
