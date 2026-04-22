"""Source-build deployment helpers for SREGym apps."""

from __future__ import annotations

import contextlib
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

from sregym.paths import TARGET_MICROSERVICES

_TRUE_VALUES = {"1", "true", "yes", "on"}
_HOTEL_IMAGE_NAME = "yinfangchen/hotelreservation"
_SOCIAL_APP_IMAGE_NAME = "deathstarbench/social-network-microservices"
_SOCIAL_NGINX_IMAGE_NAME = "yg397/openresty-thrift"
_SOCIAL_MEDIA_IMAGE_NAME = "yg397/media-frontend"
_SOCIAL_MICROSERVICE_CHARTS = (
    "compose-post-service",
    "home-timeline-service",
    "media-service",
    "post-storage-service",
    "social-graph-service",
    "text-service",
    "unique-id-service",
    "url-shorten-service",
    "user-mention-service",
    "user-service",
    "user-timeline-service",
)
_BUILT_IMAGES: set[tuple[str, str]] = set()


class SourceDeployUnsupportedError(RuntimeError):
    """Raised when source deployment is requested for an unsupported app."""


@dataclass(frozen=True)
class SourceDeploymentPlan:
    manifest_path: Path | None = None
    helm_extra_args: tuple[str, ...] = ()


class _BaseAdapter:
    app_name: str

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        raise NotImplementedError

    @contextlib.contextmanager
    def plan(
        self,
        *,
        cluster_name: str,
        node_architectures: set[str],
    ):
        self.ensure_images_loaded(cluster_name=cluster_name, node_architectures=node_architectures)
        yield SourceDeploymentPlan()


class _HotelReservationAdapter(_BaseAdapter):
    app_name = "Hotel Reservation"

    def __init__(self) -> None:
        self._source_dir = TARGET_MICROSERVICES / "hotelReservation"
        self._manifest_dir = self._source_dir / "kubernetes"

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        del node_architectures
        tag = source_image_tag(self.app_name, cluster_name)
        image_ref = f"{_HOTEL_IMAGE_NAME}:{tag}"
        if (cluster_name, image_ref) in _BUILT_IMAGES:
            return
        _run_command(
            [
                "docker",
                "build",
                "-t",
                image_ref,
                "-f",
                str(self._source_dir / "Dockerfile"),
                str(self._source_dir),
            ]
        )
        _kind_load_image(cluster_name=cluster_name, image_ref=image_ref)
        _BUILT_IMAGES.add((cluster_name, image_ref))

    @contextlib.contextmanager
    def plan(
        self,
        *,
        cluster_name: str,
        node_architectures: set[str],
    ):
        self.ensure_images_loaded(cluster_name=cluster_name, node_architectures=node_architectures)
        tag = source_image_tag(self.app_name, cluster_name)
        temp_dir = Path(tempfile.mkdtemp(prefix="sregym-hotel-src-"))
        try:
            base_dir = temp_dir / "base"
            overlay_dir = temp_dir / "overlay"
            shutil.copytree(self._manifest_dir, base_dir)
            _write_kustomization_for_directory(base_dir)
            overlay_dir.mkdir(parents=True, exist_ok=True)
            kustomization = {
                "apiVersion": "kustomize.config.k8s.io/v1beta1",
                "kind": "Kustomization",
                "resources": ["../base"],
                "images": [
                    {
                        "name": _HOTEL_IMAGE_NAME,
                        "newName": _HOTEL_IMAGE_NAME,
                        "newTag": tag,
                    }
                ],
            }
            (overlay_dir / "kustomization.yaml").write_text(
                yaml.safe_dump(kustomization, sort_keys=False),
                encoding="utf-8",
            )
            yield SourceDeploymentPlan(manifest_path=overlay_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class _SocialNetworkAdapter(_BaseAdapter):
    app_name = "Social Network"

    def __init__(self) -> None:
        self._source_dir = TARGET_MICROSERVICES / "socialNetwork"
        self._nginx_dir = self._source_dir / "docker" / "openresty-thrift"
        self._media_dir = self._source_dir / "docker" / "media-frontend"

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        tag = source_image_tag(self.app_name, cluster_name)
        media_dockerfile = (
            self._media_dir / "xenial" / "Dockerfile.arm64"
            if _is_arm(node_architectures)
            else self._media_dir / "xenial" / "Dockerfile"
        )
        build_steps = (
            (
                f"{_SOCIAL_APP_IMAGE_NAME}:{tag}",
                [
                    "docker",
                    "build",
                    "-t",
                    f"{_SOCIAL_APP_IMAGE_NAME}:{tag}",
                    str(self._source_dir),
                ],
            ),
            (
                f"{_SOCIAL_NGINX_IMAGE_NAME}:{tag}",
                [
                    "docker",
                    "build",
                    "-t",
                    f"{_SOCIAL_NGINX_IMAGE_NAME}:{tag}",
                    "-f",
                    str(self._nginx_dir / "xenial" / "Dockerfile"),
                    str(self._nginx_dir),
                ],
            ),
            (
                f"{_SOCIAL_MEDIA_IMAGE_NAME}:{tag}",
                [
                    "docker",
                    "build",
                    "-t",
                    f"{_SOCIAL_MEDIA_IMAGE_NAME}:{tag}",
                    "-f",
                    str(media_dockerfile),
                    str(self._media_dir),
                ],
            ),
        )
        for image_ref, command in build_steps:
            if (cluster_name, image_ref) in _BUILT_IMAGES:
                continue
            _run_command(command)
            _kind_load_image(cluster_name=cluster_name, image_ref=image_ref)
            _BUILT_IMAGES.add((cluster_name, image_ref))

    @contextlib.contextmanager
    def plan(
        self,
        *,
        cluster_name: str,
        node_architectures: set[str],
    ):
        self.ensure_images_loaded(cluster_name=cluster_name, node_architectures=node_architectures)
        tag = source_image_tag(self.app_name, cluster_name)
        helm_args = []
        for chart in _SOCIAL_MICROSERVICE_CHARTS:
            helm_args.extend(
                [
                    "--set-string",
                    f"{chart}.container.image={_SOCIAL_APP_IMAGE_NAME}",
                    "--set-string",
                    f"{chart}.container.imageVersion={tag}",
                ]
            )
        helm_args.extend(
            [
                "--set-string",
                f"nginx-thrift.container.image={_SOCIAL_NGINX_IMAGE_NAME}",
                "--set-string",
                f"nginx-thrift.container.imageVersion={tag}",
                "--set-string",
                f"media-frontend.container.image={_SOCIAL_MEDIA_IMAGE_NAME}",
                "--set-string",
                f"media-frontend.container.imageVersion={tag}",
            ]
        )
        yield SourceDeploymentPlan(helm_extra_args=tuple(helm_args))


_SUPPORTED_ADAPTERS: dict[str, _BaseAdapter] = {
    "Hotel Reservation": _HotelReservationAdapter(),
    "Social Network": _SocialNetworkAdapter(),
}
_UNSUPPORTED_APPS: dict[str, str] = {
    "OpenTelemetry Demo Astronomy Shop": "no local source build workflow is defined in this repo",
    "Fleet Cast": "no local source build workflow is defined in this repo",
    "Blueprint Hotel Reservation": "no local source build workflow is defined in this repo",
}


def source_deploy_enabled() -> bool:
    return os.getenv("SREGYM_DEPLOY_FROM_SOURCE", "").strip().lower() in _TRUE_VALUES


def source_image_tag(app_name: str, cluster_name: str) -> str:
    safe_app = _sanitize_identifier(app_name)
    safe_cluster = _sanitize_identifier(cluster_name)
    return f"sregym-src-{safe_app}-{safe_cluster}"


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
    with adapter.plan(cluster_name=cluster_name, node_architectures=node_architectures or set()) as plan:
        yield plan


def _is_arm(node_architectures: set[str]) -> bool:
    return any(arch in {"arm64", "aarch64"} for arch in node_architectures)


def _kind_load_image(*, cluster_name: str, image_ref: str) -> None:
    _run_command(["kind", "load", "docker-image", image_ref, "--name", cluster_name])


def _write_kustomization_for_directory(base_dir: Path) -> None:
    resources = sorted(
        str(path.relative_to(base_dir))
        for path in base_dir.rglob("*")
        if path.is_file()
        and path.suffix in {".yaml", ".yml"}
        and path.name.lower() not in {"kustomization.yaml", "kustomization.yml"}
    )
    kustomization = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": resources,
    }
    (base_dir / "kustomization.yaml").write_text(
        yaml.safe_dump(kustomization, sort_keys=False),
        encoding="utf-8",
    )


def _run_command(command: list[str]) -> None:
    subprocess.run(command, check=True)


def _sanitize_identifier(value: str) -> str:
    lowered = value.strip().lower().replace(" ", "-")
    collapsed = re.sub(r"[^a-z0-9.-]+", "-", lowered)
    return collapsed.strip("-.") or f"app-{int(time.time())}"
