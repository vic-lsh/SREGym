"""Source-build deployment helpers for SREGym apps."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from sregym.paths import TARGET_MICROSERVICES
from sregym.service.app_workspace import _astronomy_source_root, resolve_app_source_subdir
from sregym.worker_infra import kind_nodes_hold_image, tag_image_in_kind_nodes

_TRUE_VALUES = {"1", "true", "yes", "on"}
_HOTEL_IMAGE_NAME = "yinfangchen/hotelreservation"
# The tags the application source declares (hotel manifests, social chart values). A source build is built and
# loaded under a lane-private tag, then published on the nodes under the declared ref, so the deployed Deployments
# carry exactly what the source says and SDO's accepted source has nothing to explain at time zero.
_HOTEL_DECLARED_TAG = "latest"
_SOCIAL_APP_DECLARED_TAG = "latest"
_SOCIAL_NGINX_DECLARED_TAG = "xenial"
_SOCIAL_MEDIA_DECLARED_TAG = "xenial"
_ASTRONOMY_DECLARED_TAG = "2.2.0"  # the chart appVersion; the chart appends the component name
_SOCIAL_APP_IMAGE_NAME = "deathstarbench/social-network-microservices"
_SOCIAL_NGINX_IMAGE_NAME = "yg397/openresty-thrift"
_SOCIAL_MEDIA_IMAGE_NAME = "yg397/media-frontend"
_BUILT_IMAGES: set[tuple[str, str]] = set()
# Source-built images are tagged and labelled with their content identity (build
# context, Dockerfile, build arguments, platform). Opt-in (SREGYM_SOURCE_BUILD_CACHE):
# a local image carrying the identity is reused instead of rebuilt.
SOURCE_IDENTITY_LABEL = "sregym.source-identity"
# Repository metadata that changes between runs without changing the app:
# git history and SDO's operational memory.
_DIGEST_EXCLUDED_TOP_LEVEL = frozenset({".git", ".sdo", ".sdo-runtime"})


class SourceDeployUnsupportedError(RuntimeError):
    """Raised when source deployment is requested for an unsupported app."""


@dataclass(frozen=True)
class SourceDeploymentPlan:
    manifest_path: Path


@dataclass(frozen=True)
class _SourceImage:
    """One image the app source builds, and the ref its manifests declare for it."""

    repository: str
    dockerfile: Path
    context_dir: Path
    declared_tag: str
    build_args: tuple[str, ...] = ()
    suffix: str = ""

    def command(self, image_ref: str) -> list[str]:
        command = ["docker", "build", "-t", image_ref, "-f", str(self.dockerfile)]
        for argument in self.build_args:
            command.extend(["--build-arg", argument])
        return [*command, str(self.context_dir)]


def _ensure_source_image(cluster_name: str, image: _SourceImage, digests: dict[Path, str] | None = None) -> None:
    """Build (or reuse) the image by its content identity, then publish it on the lane under the declared ref.

    The built ref carries no lane name: every lane, and every host sharing a registry, resolves the same
    sources to the same ref, so a distinct image is built once.
    """
    digests = {} if digests is None else digests
    if image.context_dir not in digests:
        digests[image.context_dir] = source_context_digest(image.context_dir)
    identity = source_build_identity(digests[image.context_dir], image.context_dir, image.dockerfile, image.build_args)
    image_ref = f"{image.repository}:{source_image_tag(identity)}{image.suffix}"
    if (cluster_name, image_ref) in _BUILT_IMAGES:
        return
    _build_image(image_ref, image.command(image_ref), identity=identity)
    _kind_load_image(cluster_name=cluster_name, image_ref=image_ref)
    _declare_image(cluster_name=cluster_name, built_ref=image_ref, declared_tag=image.declared_tag)
    _BUILT_IMAGES.add((cluster_name, image_ref))


class _BaseAdapter:
    app_name: str

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        raise NotImplementedError


class _HotelReservationAdapter(_BaseAdapter):
    app_name = "Hotel Reservation"

    def __init__(self) -> None:
        self._default_source_dir = TARGET_MICROSERVICES / "hotelReservation"

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, self._default_source_dir)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        del node_architectures
        source_dir = self._source_dir()
        _ensure_source_image(
            cluster_name,
            _SourceImage(_HOTEL_IMAGE_NAME, source_dir / "Dockerfile", source_dir, _HOTEL_DECLARED_TAG),
        )


class _SocialNetworkAdapter(_BaseAdapter):
    app_name = "Social Network"

    def __init__(self) -> None:
        self._default_source_dir = TARGET_MICROSERVICES / "socialNetwork"

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, self._default_source_dir)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        source_dir = self._source_dir()
        nginx_dir = source_dir / "docker" / "openresty-thrift"
        media_dir = source_dir / "docker" / "media-frontend"
        media_dockerfile = (
            media_dir / "xenial" / "Dockerfile.arm64"
            if _is_arm(node_architectures)
            else media_dir / "xenial" / "Dockerfile"
        )
        digests: dict[Path, str] = {}
        for image in (
            _SourceImage(_SOCIAL_APP_IMAGE_NAME, source_dir / "Dockerfile", source_dir, _SOCIAL_APP_DECLARED_TAG),
            _SourceImage(
                _SOCIAL_NGINX_IMAGE_NAME, nginx_dir / "xenial" / "Dockerfile", nginx_dir, _SOCIAL_NGINX_DECLARED_TAG
            ),
            _SourceImage(_SOCIAL_MEDIA_IMAGE_NAME, media_dockerfile, media_dir, _SOCIAL_MEDIA_DECLARED_TAG),
        ):
            _ensure_source_image(cluster_name, image, digests)


class _AstronomyShopAdapter(_BaseAdapter):
    app_name = "OpenTelemetry Demo Astronomy Shop"

    # The upstream 2.2.0 Docker Compose application images. Infrastructure uses
    # the chart's pinned images; the mock LLM is built locally like every service.
    components = (
        "accounting",
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "fraud-detection",
        "frontend",
        "frontend-proxy",
        "image-provider",
        "load-generator",
        "payment",
        "product-catalog",
        "product-reviews",
        "quote",
        "recommendation",
        "shipping",
        "flagd-ui",
        "kafka",
        "llm",
    )

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, _astronomy_source_root())

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        del node_architectures  # Docker builds for the configured local builder.
        source_dir = self._source_dir()
        digests: dict[Path, str] = {}
        for component in self.components:
            dockerfile = source_dir / "src" / component / "Dockerfile"
            if component == "cart":
                dockerfile = source_dir / "src/cart/src/Dockerfile"
            build_args: tuple[str, ...] = ()
            if component in {"ad", "fraud-detection", "kafka"}:
                build_args = ("OTEL_JAVA_AGENT_VERSION=2.23.0",)
            elif component == "currency":
                build_args = ("OPENTELEMETRY_CPP_VERSION=1.24.0",)
            _ensure_source_image(
                cluster_name,
                _SourceImage(
                    "ghcr.io/open-telemetry/demo",
                    dockerfile,
                    source_dir,
                    f"{_ASTRONOMY_DECLARED_TAG}-{component}",
                    build_args=build_args,
                    suffix=f"-{component}",
                ),
                digests,
            )


_SUPPORTED_ADAPTERS: dict[str, _BaseAdapter] = {
    "Hotel Reservation": _HotelReservationAdapter(),
    "Social Network": _SocialNetworkAdapter(),
    "OpenTelemetry Demo Astronomy Shop": _AstronomyShopAdapter(),
}
_UNSUPPORTED_APPS: dict[str, str] = {
    "Fleet Cast": "no local source build workflow is defined in this repo",
    "Blueprint Hotel Reservation": "no local source build workflow is defined in this repo",
}


def source_deploy_enabled() -> bool:
    return os.getenv("SREGYM_DEPLOY_FROM_SOURCE", "").strip().lower() in _TRUE_VALUES


def source_build_cache_enabled() -> bool:
    return os.getenv("SREGYM_SOURCE_BUILD_CACHE", "").strip().lower() in _TRUE_VALUES


def source_context_digest(context_dir: Path) -> str:
    """SHA-256 over the build context's paths, modes, and contents.

    Git history and ``.sdo`` operational memory at the context root are
    excluded: they change on every run without changing the application.
    """
    digest = hashlib.sha256()
    root = Path(context_dir)
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.parts[0] in _DIGEST_EXCLUDED_TOP_LEVEL:
            continue
        name = relative.as_posix().encode()
        if path.is_symlink():
            digest.update(b"L\0" + name + b"\0" + os.readlink(path).encode() + b"\0")
        elif path.is_file():
            executable = b"x" if os.access(path, os.X_OK) else b"-"
            digest.update(b"F\0" + name + b"\0" + executable + b"\0")
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
            digest.update(b"\0")
        elif path.is_dir():
            digest.update(b"D\0" + name + b"\0")
    return digest.hexdigest()


def source_build_identity(context_digest: str, context_dir: Path, dockerfile: Path, build_args: tuple[str, ...]) -> str:
    """Content identity of one source build: everything that decides the image bytes, and no lane or path."""
    try:
        recipe = "context:" + Path(dockerfile).relative_to(context_dir).as_posix()
    except ValueError:
        recipe = "sha256:" + hashlib.sha256(Path(dockerfile).read_bytes()).hexdigest()
    payload = {
        "version": 1,
        "context": context_digest,
        "dockerfile": recipe,
        "build_args": list(build_args),
        "platform": _docker_platform(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _docker_platform() -> str:
    machine = platform.machine().lower()
    return "linux/arm64" if machine in {"arm64", "aarch64"} else "linux/amd64"


def local_image_identity(image_ref: str) -> str | None:
    """The content identity a local image was built from, or None when absent or unlabelled."""
    template = '{{ index .Config.Labels "' + SOURCE_IDENTITY_LABEL + '" }}'
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", template, image_ref],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _build_image(image_ref: str, command: list[str], *, identity: str) -> None:
    """Build ``image_ref`` labelled with its identity, or reuse the local image already built from it."""
    if source_build_cache_enabled() and local_image_identity(image_ref) == identity:
        print(f"Reusing {image_ref}: built from identity {identity[:12]}")
        return
    _run_command([*command[:2], "--label", f"{SOURCE_IDENTITY_LABEL}={identity}", *command[2:]])


def source_image_tag(identity: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError(f"source image identity must be a sha256 hex digest: {identity!r}")
    return f"sregym-src-{identity}"


def unsupported_reason(app_name: str) -> str | None:
    if app_name in _SUPPORTED_ADAPTERS:
        return None
    return _UNSUPPORTED_APPS.get(app_name, "no source deployment adapter is registered")


def ensure_app_supported(app_name: str) -> None:
    reason = unsupported_reason(app_name)
    if reason is not None:
        raise SourceDeployUnsupportedError(f"Source deploy is not supported for '{app_name}': {reason}")


def is_supported(app_name: str) -> bool:
    return app_name in _SUPPORTED_ADAPTERS


def all_declared_apps() -> set[str]:
    return set(_SUPPORTED_ADAPTERS) | set(_UNSUPPORTED_APPS)


@contextlib.contextmanager
def plan_for_app(app, *, node_architectures: set[str] | None = None):
    ensure_app_supported(app.name)
    cluster_name = os.getenv("SREGYM_KIND_CLUSTER_NAME", "").strip()
    if not cluster_name:
        raise RuntimeError("Source deploy requires SREGYM_KIND_CLUSTER_NAME to be set")
    adapter = _SUPPORTED_ADAPTERS[app.name]
    rendered = os.getenv("SREGYM_ACCEPTED_RENDER", "")
    if not rendered or not Path(rendered).is_file():
        raise SourceDeployUnsupportedError(
            "source deployment requires the accepted revision render (SREGYM_ACCEPTED_RENDER)"
        )
    adapter.ensure_images_loaded(cluster_name=cluster_name, node_architectures=node_architectures or set())
    yield SourceDeploymentPlan(manifest_path=Path(rendered))


def _is_arm(node_architectures: set[str]) -> bool:
    return any(arch in {"arm64", "aarch64"} for arch in node_architectures)


def _kind_load_image(*, cluster_name: str, image_ref: str) -> None:
    # With the build cache on, an image reused from an unchanged context is already on the nodes.
    if source_build_cache_enabled() and kind_nodes_hold_image(cluster_name, image_ref):
        print(f"Skipping kind load of {image_ref}: every node already holds it")
        return
    _run_command(["kind", "load", "docker-image", image_ref, "--name", cluster_name])


def _declare_image(*, cluster_name: str, built_ref: str, declared_tag: str) -> None:
    """Publish the lane-built image on the nodes under the tag the source declares (idempotent)."""
    repository = built_ref.rpartition(":")[0]
    tag_image_in_kind_nodes(cluster_name, built_ref, f"{repository}:{declared_tag}")


def _run_command(command: list[str]) -> None:
    builder = os.getenv("SREGYM_DOCKER_BUILDER", "").strip()
    if builder and command[:2] == ["docker", "build"]:
        command = ["docker", "buildx", "build", "--builder", builder, "--load", *command[2:]]
    subprocess.run(command, check=True)


def _sanitize_identifier(value: str) -> str:
    lowered = value.strip().lower().replace(" ", "-")
    collapsed = re.sub(r"[^a-z0-9.-]+", "-", lowered)
    return collapsed.strip("-.") or f"app-{int(time.time())}"


def _resolve_app_source_dir(app_name: str, default_dir: Path) -> Path:
    override = os.getenv("SREGYM_APP_SOURCE_DIR", "").strip()
    if not override:
        return default_dir
    try:
        app_filter = (
            "astronomy_shop"
            if app_name == "OpenTelemetry Demo Astronomy Shop"
            else _sanitize_identifier(app_name).replace("-", "_")
        )
        resolve_app_source_subdir(app_filter)
    except ValueError:
        return default_dir
    return Path(override)
