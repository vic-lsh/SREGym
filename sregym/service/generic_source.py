"""Opt-in declaration-driven source builds; legacy adapters remain independent."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import zip_longest
from pathlib import Path

import yaml


def _relative(value: str) -> str:
    if not isinstance(value, str) or not value or Path(value).is_absolute() or ".." in Path(value).parts:
        raise ValueError(f"Expected a relative path within the source: {value!r}")
    return value


def _inside(root: Path, value: str) -> Path:
    path = (root / _relative(value)).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Path escapes source root: {value}")
    return path


def _text(value: str, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")


@dataclass(frozen=True)
class Download:
    url: str
    sha256: str
    replace: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.url, str) or not self.url.startswith(("https://", "http://", "file://")):
            raise ValueError("download URL must use https, http, or file")
        if not isinstance(self.sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("download sha256 must be 64 lowercase hex digits")
        if self.replace is not None:
            _text(self.replace, "download replacement URL")

    @property
    def key(self) -> str:
        return hashlib.sha256(f"{self.url}\n{self.sha256}".encode()).hexdigest()


class DownloadCache:
    """Atomic, verified URL+sha256 cache shared across apps and processes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def fetch(self, download: Download) -> Path:
        target = self.root / f"{download.key}.blob"
        with (self.root / f"{download.key}.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if target.exists():
                if self._digest(target) != download.sha256:
                    raise RuntimeError(f"Download cache sha256 mismatch: {target}; remove corrupt entry and retry")
                return target
            with tempfile.NamedTemporaryFile(dir=self.root, delete=False) as handle:
                temporary = Path(handle.name)
                try:
                    with urllib.request.urlopen(download.url, timeout=60) as response:
                        shutil.copyfileobj(response, handle)
                    handle.flush()
                    if self._digest(temporary) != download.sha256:
                        raise RuntimeError(f"Download sha256 mismatch for {download.url}: expected {download.sha256}")
                    temporary.replace(target)
                except (OSError, urllib.error.URLError) as exc:
                    raise RuntimeError(
                        f"Download unavailable on cold cache: {download.url}; expected sha256 {download.sha256}; cache {target}"
                    ) from exc
                finally:
                    temporary.unlink(missing_ok=True)
        return target

    @staticmethod
    def _digest(path: Path) -> str:
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()


@dataclass(frozen=True)
class Image:
    name: str
    context: str
    dockerfile: str
    suffix: str = ""
    dockerfile_arm64: str | None = None
    build_args: dict[str, str] = field(default_factory=dict)
    target: str | None = None
    helm_image_keys: tuple[str, ...] = ()
    helm_tag_keys: tuple[str, ...] = ()
    downloads: tuple[Download, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not re.fullmatch(r"[a-z0-9][a-z0-9./_-]*", self.name):
            raise ValueError(f"Invalid image repository: {self.name!r}")
        _relative(self.context)
        _relative(self.dockerfile)
        if self.dockerfile_arm64 is not None:
            _relative(self.dockerfile_arm64)
        if not re.fullmatch(r"[a-zA-Z0-9_.-]*", self.suffix):
            raise ValueError("Invalid image suffix")
        if not isinstance(self.build_args, dict) or any(
            not isinstance(k, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", k) or not isinstance(v, str)
            for k, v in self.build_args.items()
        ):
            raise ValueError("build_args must map argument names to strings")
        if self.target is not None and not re.fullmatch(r"[a-zA-Z0-9_.-]+", self.target):
            raise ValueError("Invalid build target")
        for key in (*self.helm_image_keys, *self.helm_tag_keys):
            if not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]+", key):
                raise ValueError("Invalid Helm image/tag key")


@dataclass(frozen=True)
class Deployment:
    kind: str
    path: str
    namespace: str
    root: str = "source"
    helm_tag_keys: tuple[str, ...] = ()
    url: str | None = None
    ref: str | None = None

    def __post_init__(self) -> None:
        if self.ref is not None and not re.fullmatch(r"[a-f0-9]{40}", self.ref):
            raise ValueError("deployment ref must be a pinned Git commit")
        if self.kind not in {"helm", "manifest"} or self.root not in {"source", "harness"}:
            raise ValueError("deployment kind must be helm or manifest; root must be source or harness")
        _relative(self.path)
        if not isinstance(self.namespace, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", self.namespace):
            raise ValueError("Invalid deployment namespace")
        for key in self.helm_tag_keys:
            if not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]+", key):
                raise ValueError("Invalid Helm tag key")


@dataclass(frozen=True)
class Declaration:
    name: str
    app_filter: str
    source_root: Path
    source_url: str
    source_ref: str
    images: tuple[Image, ...]
    deployment: Deployment
    image_tag: str = "{tag}"

    def __post_init__(self) -> None:
        _text(self.name, "app name")
        if not re.fullmatch(r"[a-z0-9_]+", self.app_filter):
            raise ValueError("Invalid app filter")
        _text(self.source_url, "source url")
        if not re.fullmatch(r"[a-f0-9]{40}", self.source_ref):
            raise ValueError("source ref must be a pinned Git commit")
        if self.image_tag != "{tag}":
            raise ValueError("version 1 image_tag must be {tag}; component suffixes belong on images")
        if not self.images or len({(i.name, i.suffix) for i in self.images}) != len(self.images):
            raise ValueError("images must be nonempty and unique")


def load_declaration(path: Path) -> Declaration:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or (type(data.get("version")) is not int or data.get("version") != 1):
        raise ValueError(f"Unsupported build declaration version: {path}")
    if set(data) - {"version", "app", "source", "images", "deployment", "image_tag"}:
        raise ValueError("Unknown build declaration fields")
    try:
        images = []
        for raw in data["images"]:
            image = dict(raw)
            image["downloads"] = tuple(Download(**d) for d in image.get("downloads", []))
            images.append(Image(**image))
        return Declaration(
            name=data["app"]["name"],
            app_filter=data["app"]["filter"],
            source_root=(path.parent / data["source"]["path"]).resolve(),
            source_url=data["source"]["url"],
            source_ref=data["source"]["ref"],
            image_tag=data["image_tag"],
            images=tuple(images),
            deployment=Deployment(**data["deployment"]),
        )
    except (KeyError, AttributeError) as exc:
        raise ValueError(f"Incomplete build declaration: {path}: {exc}") from exc


def configured_declaration() -> Declaration | None:
    path = os.getenv("SREGYM_APP_BUILD_DECLARATION", "").strip()
    return load_declaration(Path(path)) if path else None


def build_workers(requested: int) -> int:
    if not isinstance(requested, int) or isinstance(requested, bool) or requested < 1 or requested > 4:
        raise ValueError("SREGYM_SOURCE_BUILD_JOBS must be between 1 and 4")
    available = max(1, int((os.cpu_count() or 1) - os.getloadavg()[0]))
    return min(requested, available)


@dataclass(frozen=True)
class BuildStep:
    image_ref: str
    command: list[str]
    context: Path
    image: Image


class GenericSourceAdapter:
    def __init__(self, declaration: Declaration, *, source_root: Path | None = None) -> None:
        self.declaration = declaration
        self.app_name = declaration.name
        self.source_root = (
            source_root or Path(os.getenv("SREGYM_APP_SOURCE_DIR") or declaration.source_root)
        ).resolve()

    def build_steps(self, cluster_name: str, node_architectures: set[str]) -> tuple[BuildStep, ...]:
        from sregym.service.source_deploy import source_image_tag

        tag = source_image_tag(self.app_name, cluster_name)
        steps = []
        for image in self.declaration.images:
            dockerfile = (
                image.dockerfile_arm64
                if image.dockerfile_arm64 and node_architectures & {"arm64", "aarch64"}
                else image.dockerfile
            )
            context = _inside(self.source_root, image.context)
            ref = f"{image.name}:{tag}{image.suffix}"
            command = ["docker", "build", "-t", ref, "-f", str(_inside(self.source_root, dockerfile))]
            for key, value in image.build_args.items():
                command.extend(["--build-arg", f"{key}={value}"])
            if image.target:
                command.extend(["--target", image.target])
            command.append(str(context))
            steps.append(BuildStep(ref, command, context, image))
        return tuple(steps)

    def ensure_images_loaded(self, *, cluster_name: str, node_architectures: set[str]) -> None:
        from sregym.service import source_deploy as legacy

        steps = self.build_steps(cluster_name, node_architectures)
        workers = build_workers(int(os.getenv("SREGYM_SOURCE_BUILD_JOBS", "1")))
        # Validate every input before any Docker command. No Dockerfile discovery.
        for step in steps:
            if not step.context.is_dir() or not Path(step.command[step.command.index("-f") + 1]).is_file():
                raise ValueError(f"Declared build input missing for {step.image_ref}: {step.context}")
        cache = DownloadCache(Path(os.getenv("SREGYM_DOWNLOAD_CACHE", "~/.cache/sregym/downloads")).expanduser())
        # Hash a shared context once, even for astronomy's twenty images.
        digests = (
            {
                s.context: legacy.source_context_digest(s.context)
                for s in {step.context: step for step in steps}.values()
            }
            if legacy.source_build_cache_enabled()
            else {}
        )

        def build(step: BuildStep) -> None:
            with _cached_dockerfile(step, cache) as command:
                fingerprint = hashlib.sha256(
                    json.dumps(
                        [
                            digests.get(step.context),
                            step.command,
                            Path(command[command.index("-f") + 1]).read_text(),
                            [(d.url, d.sha256) for d in step.image.downloads],
                        ],
                        sort_keys=True,
                    ).encode()
                ).hexdigest()
                if (
                    not legacy.source_build_cache_enabled()
                    or legacy._local_image_digest_label(step.image_ref) != fingerprint
                ):
                    legacy._run_command(
                        [*command[:2], "--label", f"{legacy._SOURCE_DIGEST_LABEL}={fingerprint}", *command[2:]]
                    )
                legacy._kind_load_image(cluster_name=cluster_name, image_ref=step.image_ref)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Consume all results so failures always propagate; executor joins workers.
            list(pool.map(build, steps))

    @contextlib.contextmanager
    def plan(self, *, cluster_name: str, node_architectures: set[str]):
        from sregym.paths import TARGET_MICROSERVICES
        from sregym.service.source_deploy import (
            SourceDeploymentPlan,
            _write_kustomization_for_directory,
            source_image_tag,
        )

        deployment = self.declaration.deployment
        root = self.source_root if deployment.root == "source" else TARGET_MICROSERVICES
        path = _inside(root, deployment.path)
        if not path.is_dir() and not (deployment.kind == "manifest" and path.is_file()):
            raise ValueError(f"Declared deployment source is missing: {path}")
        self.ensure_images_loaded(cluster_name=cluster_name, node_architectures=node_architectures)
        tag = source_image_tag(self.app_name, cluster_name)
        if deployment.kind == "helm":
            args = []
            for image in self.declaration.images:
                for image_key, tag_key in zip_longest(image.helm_image_keys, image.helm_tag_keys):
                    if image_key:
                        args.extend(["--set-string", f"{image_key}={image.name}"])
                    if tag_key:
                        args.extend(["--set-string", f"{tag_key}={tag}{image.suffix}"])
            for key in deployment.helm_tag_keys:
                args.extend(["--set-string", f"{key}={tag}"])
            yield SourceDeploymentPlan(
                helm_extra_args=tuple(args), helm_chart_path=path, namespace=deployment.namespace
            )
        else:
            with tempfile.TemporaryDirectory(prefix="sregym-generic-src-") as temporary:
                base = Path(temporary) / "base"
                overlay = Path(temporary) / "overlay"
                if path.is_dir():
                    shutil.copytree(path, base)
                else:
                    base.mkdir()
                    shutil.copyfile(path, base / path.name)
                if not (base / "kustomization.yaml").exists() and not (base / "kustomization.yml").exists():
                    _write_kustomization_for_directory(base)
                overlay.mkdir()
                (overlay / "kustomization.yaml").write_text(
                    yaml.safe_dump(
                        {
                            "apiVersion": "kustomize.config.k8s.io/v1beta1",
                            "kind": "Kustomization",
                            "resources": ["../base"],
                            "images": [
                                {"name": i.name, "newName": i.name, "newTag": tag + i.suffix}
                                for i in self.declaration.images
                            ],
                        },
                        sort_keys=False,
                    )
                )
                yield SourceDeploymentPlan(manifest_path=overlay, namespace=deployment.namespace)


@contextlib.contextmanager
def _cached_dockerfile(step: BuildStep, cache: DownloadCache):
    """Replace declared archive fetches with verified local BuildKit context copies.

    Keep upstream source untouched. Package-manager caches remain Docker/BuildKit's
    responsibility; explicit curl/wget archive inputs are pinned in the declaration.
    """
    if not step.image.downloads:
        yield step.command
        return
    original = Path(step.command[step.command.index("-f") + 1])
    content = original.read_text()
    with tempfile.TemporaryDirectory(prefix="sregym-download-context-") as directory:
        root = Path(directory)
        copies = []
        for download in step.image.downloads:
            shutil.copyfile(cache.fetch(download), root / download.key)
            url = re.escape(download.replace or download.url)
            # Restrict v1 to the two archive-fetch forms used by the app inputs.
            pattern = rf"(?:curl\s+-fSL\s+{url}\s+-o\s+([^\s;&\\]+)|wget\s+{url})"

            def replace(
                match: re.Match[str], key: str = download.key, source_url: str = download.replace or download.url
            ) -> str:
                destination = match.group(1) or source_url.rsplit("/", 1)[-1]
                return f"cp /.sregym-downloads/{key} {destination}"

            content, count = re.subn(pattern, replace, content)
            if count != 1:
                raise ValueError(
                    f"Declared download must match exactly one curl/wget fetch in {original}: {download.replace or download.url}"
                )
            copies.append(f"COPY --from=sregym-downloads {download.key} /.sregym-downloads/{download.key}")
        # Every stage can use the cached archives; no daemon HTTP server required.
        content = re.sub(r"(?im)^(FROM [^\n]+)$", lambda m: m.group(0) + "\n" + "\n".join(copies), content)
        generated = root / "Dockerfile"
        generated.write_text(content)
        command = list(step.command)
        command[command.index("-f") + 1] = str(generated)
        command[2:2] = ["--build-context", f"sregym-downloads={root}"]
        yield command
