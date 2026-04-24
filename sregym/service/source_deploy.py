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
from sregym.service.app_workspace import resolve_app_source_subdir

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
_TRAIN_TICKET_IMAGE_REPO = "ghcr.io/sregym"
_TRAIN_TICKET_DEPLOY_IMAGE_NAME = f"{_TRAIN_TICKET_IMAGE_REPO}/train-ticket-deploy"
_TRAIN_TICKET_AVATAR_BASE_IMAGE_NAME = f"{_TRAIN_TICKET_IMAGE_REPO}/ts-avatar-service-base"
_TRAIN_TICKET_JAVA_SERVICES = (
    "ts-admin-basic-info-service",
    "ts-admin-order-service",
    "ts-admin-route-service",
    "ts-admin-travel-service",
    "ts-admin-user-service",
    "ts-assurance-service",
    "ts-auth-service",
    "ts-basic-service",
    "ts-cancel-service",
    "ts-config-service",
    "ts-consign-price-service",
    "ts-consign-service",
    "ts-contacts-service",
    "ts-delivery-service",
    "ts-execute-service",
    "ts-food-delivery-service",
    "ts-food-service",
    "ts-gateway-service",
    "ts-inside-payment-service",
    "ts-notification-service",
    "ts-order-other-service",
    "ts-order-service",
    "ts-payment-service",
    "ts-preserve-other-service",
    "ts-preserve-service",
    "ts-price-service",
    "ts-rebook-service",
    "ts-route-plan-service",
    "ts-route-service",
    "ts-seat-service",
    "ts-security-service",
    "ts-station-food-service",
    "ts-station-service",
    "ts-train-food-service",
    "ts-train-service",
    "ts-travel-plan-service",
    "ts-travel-service",
    "ts-travel2-service",
    "ts-user-service",
    "ts-verification-code-service",
    "ts-wait-order-service",
)
_TRAIN_TICKET_NON_JAVA_SERVICES = (
    "ts-avatar-service",
    "ts-news-service",
    "ts-ticket-office-service",
    "ts-ui-dashboard",
    "ts-voucher-service",
)
_TRAIN_TICKET_SERVICES = _TRAIN_TICKET_JAVA_SERVICES + _TRAIN_TICKET_NON_JAVA_SERVICES
_TRAIN_TICKET_CIRCULAR_REF_ENV = {
    "name": "SPRING_MAIN_ALLOW_CIRCULAR_REFERENCES",
    "value": "true",
}
_TRAIN_TICKET_MANIFEST_PATHS = (
    Path("deployment/kubernetes-manifests/quickstart-k8s/yamls/deploy.yaml.sample"),
    Path("deployment/kubernetes-manifests/quickstart-k8s/yamls/sw_deploy.yaml.sample"),
)
_TRAIN_TICKET_MAVEN_WRAPPER_CANDIDATES = (
    Path("mvnw"),
    Path("ts-travel-service/mvnw"),
    Path("ts-payment-service/mvnw"),
    Path("ts-notification-service/mvnw"),
)
_DOCKERIZED_MAVEN_IMAGE = "maven:3.9.9-eclipse-temurin-8"
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
        self._default_source_dir = TARGET_MICROSERVICES / "hotelReservation"

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, self._default_source_dir)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        del node_architectures
        tag = source_image_tag(self.app_name, cluster_name)
        image_ref = f"{_HOTEL_IMAGE_NAME}:{tag}"
        if (cluster_name, image_ref) in _BUILT_IMAGES:
            return
        source_dir = self._source_dir()
        _run_command(
            [
                "docker",
                "build",
                "-t",
                image_ref,
                "-f",
                str(source_dir / "Dockerfile"),
                str(source_dir),
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
            source_dir = self._source_dir()
            base_dir = temp_dir / "base"
            overlay_dir = temp_dir / "overlay"
            shutil.copytree(source_dir / "kubernetes", base_dir)
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
        self._default_source_dir = TARGET_MICROSERVICES / "socialNetwork"

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, self._default_source_dir)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        tag = source_image_tag(self.app_name, cluster_name)
        source_dir = self._source_dir()
        nginx_dir = source_dir / "docker" / "openresty-thrift"
        media_dir = source_dir / "docker" / "media-frontend"
        media_dockerfile = (
            media_dir / "xenial" / "Dockerfile.arm64"
            if _is_arm(node_architectures)
            else media_dir / "xenial" / "Dockerfile"
        )
        build_steps = (
            (
                f"{_SOCIAL_APP_IMAGE_NAME}:{tag}",
                [
                    "docker",
                    "build",
                    "-t",
                    f"{_SOCIAL_APP_IMAGE_NAME}:{tag}",
                    str(source_dir),
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
                    str(nginx_dir / "xenial" / "Dockerfile"),
                    str(nginx_dir),
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
                    str(media_dir),
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


class _TrainTicketAdapter(_BaseAdapter):
    app_name = "Train Ticket"

    def __init__(self) -> None:
        self._default_source_dir = TARGET_MICROSERVICES / "train-ticket"

    def _source_dir(self) -> Path:
        return _resolve_app_source_dir(self.app_name, self._default_source_dir)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        del node_architectures
        tag = source_image_tag(self.app_name, cluster_name)
        source_dir = self._source_dir()
        missing_services = [
            service
            for service in _TRAIN_TICKET_SERVICES
            if (cluster_name, _train_ticket_image_ref(service, tag)) not in _BUILT_IMAGES
        ]
        if not missing_services:
            return

        java_missing = [service for service in missing_services if service in _TRAIN_TICKET_JAVA_SERVICES]
        if java_missing:
            command, cwd = _maven_invocation(source_dir, ["clean", "install", "-DskipTests", "-N"], source_dir / "pom.xml")
            _run_command_in_cwd(command, cwd)
            command, cwd = _maven_invocation(
                source_dir,
                ["clean", "install", "-DskipTests"],
                source_dir / "ts-common" / "pom.xml",
            )
            _run_command_in_cwd(command, cwd)

        for service in missing_services:
            if service in _TRAIN_TICKET_JAVA_SERVICES:
                command, cwd = _maven_invocation(
                    source_dir,
                    ["clean", "package", "-DskipTests"],
                    source_dir / service / "pom.xml",
                )
                _run_command_in_cwd(command, cwd)

            image_ref = _train_ticket_image_ref(service, tag)
            if service == "ts-avatar-service":
                avatar_base_image_ref = _train_ticket_avatar_base_image_ref(tag)
                if (cluster_name, avatar_base_image_ref) not in _BUILT_IMAGES:
                    _run_command(
                        [
                            "docker",
                            "build",
                            "--target",
                            "avatar-base",
                            "-t",
                            avatar_base_image_ref,
                            str(source_dir / service),
                        ]
                    )
                    _BUILT_IMAGES.add((cluster_name, avatar_base_image_ref))
                _run_command(
                    [
                        "docker",
                        "build",
                        "--build-arg",
                        f"AVATAR_BASE_IMAGE={avatar_base_image_ref}",
                        "-t",
                        image_ref,
                        str(source_dir / service),
                    ]
                )
            else:
                _run_command(["docker", "build", "-t", image_ref, str(source_dir / service)])
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
        deploy_image_ref = f"{_TRAIN_TICKET_DEPLOY_IMAGE_NAME}:{tag}"
        temp_dir = Path(tempfile.mkdtemp(prefix="sregym-train-ticket-src-"))
        try:
            source_dir = self._source_dir()
            build_context = temp_dir / "deploy-job"
            shutil.copytree(source_dir / "deploy-job", build_context)
            shutil.copytree(source_dir / "deployment", build_context / "deployment", dirs_exist_ok=True)

            for manifest_rel_path in _TRAIN_TICKET_MANIFEST_PATHS:
                _rewrite_train_ticket_manifest_images(build_context / manifest_rel_path, tag)

            if (cluster_name, deploy_image_ref) not in _BUILT_IMAGES:
                _run_command(["docker", "build", "-t", deploy_image_ref, str(build_context)])
                _kind_load_image(cluster_name=cluster_name, image_ref=deploy_image_ref)
                _BUILT_IMAGES.add((cluster_name, deploy_image_ref))

            yield SourceDeploymentPlan(
                helm_extra_args=(
                    "--set-string",
                    f"job.image={deploy_image_ref}",
                    "--set-string",
                    "job.imagePullPolicy=IfNotPresent",
                )
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


_SUPPORTED_ADAPTERS: dict[str, _BaseAdapter] = {
    "Hotel Reservation": _HotelReservationAdapter(),
    "Social Network": _SocialNetworkAdapter(),
    "Train Ticket": _TrainTicketAdapter(),
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


def _run_command_in_cwd(command: list[str], cwd: Path) -> None:
    subprocess.run(command, check=True, cwd=cwd)


def _maven_invocation(source_dir: Path, goals: list[str], pom_path: Path) -> tuple[list[str], Path]:
    if shutil.which("java"):
        for candidate in _TRAIN_TICKET_MAVEN_WRAPPER_CANDIDATES:
            wrapper_path = source_dir / candidate
            if wrapper_path.is_file():
                return ["sh", str(wrapper_path), "-f", str(pom_path), *goals], wrapper_path.parent

        if shutil.which("mvn"):
            return ["mvn", *goals, "-f", str(pom_path)], source_dir

    cache_dir = _train_ticket_maven_cache_dir(source_dir)
    workspace_pom = Path("/workspace") / pom_path.relative_to(source_dir)
    return [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{source_dir}:/workspace",
        "-v",
        f"{cache_dir}:/root/.m2",
        "-w",
        "/workspace",
        _DOCKERIZED_MAVEN_IMAGE,
        "mvn",
        "-f",
        str(workspace_pom),
        *goals,
    ], source_dir


def _train_ticket_maven_cache_dir(source_dir: Path) -> Path:
    cache_dir = Path(tempfile.gettempdir()) / "sregym-train-ticket-m2" / _sanitize_identifier(str(source_dir))
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _train_ticket_image_ref(service: str, tag: str) -> str:
    return f"{_TRAIN_TICKET_IMAGE_REPO}/{service}:{tag}"


def _train_ticket_avatar_base_image_ref(tag: str) -> str:
    return f"{_TRAIN_TICKET_AVATAR_BASE_IMAGE_NAME}:{tag}"


def _rewrite_train_ticket_manifest_images(manifest_path: Path, tag: str) -> None:
    content = manifest_path.read_text(encoding="utf-8")
    for service in _TRAIN_TICKET_SERVICES:
        content = re.sub(
            rf"((?:codewisdom|ghcr\.io/sregym)/{re.escape(service)}):[^\s\"']+",
            _train_ticket_image_ref(service, tag),
            content,
        )

    documents = list(yaml.safe_load_all(content))
    for document in documents:
        if not isinstance(document, dict):
            continue
        if document.get("kind") != "Deployment":
            continue
        metadata = document.get("metadata") or {}
        service_name = metadata.get("name")
        if service_name not in _TRAIN_TICKET_JAVA_SERVICES:
            continue
        template_spec = (((document.get("spec") or {}).get("template") or {}).get("spec") or {})
        for container in template_spec.get("containers") or []:
            if container.get("name") != service_name:
                continue
            env = container.setdefault("env", [])
            if not any(item.get("name") == _TRAIN_TICKET_CIRCULAR_REF_ENV["name"] for item in env):
                env.append(dict(_TRAIN_TICKET_CIRCULAR_REF_ENV))

    manifest_path.write_text(yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8")


def _sanitize_identifier(value: str) -> str:
    lowered = value.strip().lower().replace(" ", "-")
    collapsed = re.sub(r"[^a-z0-9.-]+", "-", lowered)
    return collapsed.strip("-.") or f"app-{int(time.time())}"


def _resolve_app_source_dir(app_name: str, default_dir: Path) -> Path:
    override = os.getenv("SREGYM_APP_SOURCE_DIR", "").strip()
    if not override:
        return default_dir
    override_dir = Path(override)
    try:
        expected_name = resolve_app_source_subdir(_sanitize_identifier(app_name).replace("-", "_"))
    except ValueError:
        return default_dir
    if override_dir.name != expected_name:
        return default_dir
    return override_dir
