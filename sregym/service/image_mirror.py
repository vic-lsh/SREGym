"""Rewrite container image references to a registry mirror (for clusters without ``kind load``).

Off unless configured, so kind lanes are unchanged. Two forms, combinable:

* ``SREGYM_IMAGE_MIRROR_PREFIX=<registry>/<path>``: every image moves under one prefix keeping its
  canonical registry and path, ``mongo:4.4.6`` -> ``<prefix>/docker.io/library/mongo:4.4.6``.
  ``harness/mirror_images.sh`` fills a private ECR registry in exactly this layout.
* ``SREGYM_IMAGE_MIRRORS=docker.io=<ecr>/dockerhub,quay.io=<ecr>/quay``: per-registry prefixes in
  the layout of an ECR pull-through cache rule, ``mongo:4.4.6`` -> ``<ecr>/dockerhub/library/mongo:4.4.6``.
  A per-registry entry wins over the global prefix.

Images already in ECR (``*.amazonaws.com``, ``public.ecr.aws``) are never rewritten. Rewritten
fields: every ``image`` string in a manifest and env values whose name ends in ``_IMAGE`` (OpenEBS
names its helper image that way).

Run as ``python -m sregym.service.image_mirror`` it is a Helm post-renderer: manifests on stdin,
rewritten manifests on stdout.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PREFIX_ENV = "SREGYM_IMAGE_MIRROR_PREFIX"
MIRRORS_ENV = "SREGYM_IMAGE_MIRRORS"
DOCKER_HUB = "docker.io"
_NEVER_REWRITTEN_SUFFIXES = (".amazonaws.com", "public.ecr.aws")


@dataclass(frozen=True)
class ImageRef:
    registry: str
    path: str
    tag: str | None
    digest: str | None

    @property
    def suffix(self) -> str:
        if self.digest:
            return f"@{self.digest}"
        return f":{self.tag or 'latest'}"

    @property
    def canonical(self) -> str:
        return f"{self.registry}/{self.path}{self.suffix}"


def parse_image(ref: str) -> ImageRef:
    """Normalize a Docker image reference the way containerd resolves it."""
    if not ref or any(ch.isspace() for ch in ref):
        raise ValueError(f"not an image reference: {ref!r}")
    name, digest = (ref.split("@", 1) + [None])[:2] if "@" in ref else (ref, None)
    tag = None
    last = name.rsplit("/", 1)[-1]
    if ":" in last:
        name, tag = name.rsplit(":", 1)
    first, _, rest = name.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        registry, path = first, rest
    else:
        registry, path = DOCKER_HUB, name
    if registry in {"index.docker.io", "registry-1.docker.io"}:
        registry = DOCKER_HUB
    if registry == DOCKER_HUB and "/" not in path:
        path = f"library/{path}"
    return ImageRef(registry=registry, path=path, tag=tag, digest=digest)


def mirror_config(environ: dict[str, str] | None = None) -> tuple[str, dict[str, str]]:
    env = os.environ if environ is None else environ
    prefix = env.get(PREFIX_ENV, "").strip().rstrip("/")
    per_registry: dict[str, str] = {}
    for item in env.get(MIRRORS_ENV, "").split(","):
        item = item.strip()
        if not item:
            continue
        registry, sep, target = item.partition("=")
        if not sep or not registry.strip() or not target.strip():
            raise ValueError(f"{MIRRORS_ENV} entries must look like registry=prefix, got {item!r}")
        per_registry[registry.strip()] = target.strip().rstrip("/")
    return prefix, per_registry


def mirroring_enabled(environ: dict[str, str] | None = None) -> bool:
    prefix, per_registry = mirror_config(environ)
    return bool(prefix or per_registry)


def rewrite_image(ref: str, environ: dict[str, str] | None = None) -> str:
    prefix, per_registry = mirror_config(environ)
    if not prefix and not per_registry:
        return ref
    try:
        image = parse_image(ref)
    except ValueError:
        return ref
    if image.registry.endswith(_NEVER_REWRITTEN_SUFFIXES):
        return ref
    for mirrored in [prefix, *per_registry.values()]:
        if mirrored and ref.startswith(mirrored + "/"):
            return ref  # already rewritten
    if image.registry in per_registry:
        return f"{per_registry[image.registry]}/{image.path}{image.suffix}"
    if prefix:
        return f"{prefix}/{image.canonical}"
    return ref


def _rewrite_node(node: Any, environ: dict[str, str] | None) -> Any:
    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            if key == "image" and isinstance(value, str):
                out[key] = rewrite_image(value, environ)
            else:
                out[key] = _rewrite_node(value, environ)
        name = out.get("name")
        if isinstance(name, str) and name.endswith("_IMAGE") and isinstance(out.get("value"), str):
            out["value"] = rewrite_image(out["value"], environ)
        return out
    if isinstance(node, list):
        return [_rewrite_node(item, environ) for item in node]
    return node


def rewrite_manifest_text(text: str, environ: dict[str, str] | None = None) -> str:
    """Rewrite every image in a (multi-document) YAML or JSON manifest; unchanged text when off."""
    if not mirroring_enabled(environ):
        return text
    documents = [doc for doc in yaml.safe_load_all(text) if doc is not None]
    rewritten = [_rewrite_node(doc, environ) for doc in documents]
    return yaml.safe_dump_all(rewritten, sort_keys=False)


def rewrite_object(obj: Any, environ: dict[str, str] | None = None) -> Any:
    """Rewrite images in an already-parsed manifest (dict or list)."""
    if not mirroring_enabled(environ):
        return obj
    return _rewrite_node(obj, environ)


@contextmanager
def mirrored_manifest(source: str | Path) -> Iterator[str]:
    """Yield a path (or the URL itself when mirroring is off) holding the manifest with images rewritten.

    ``source`` is a local file or an ``https://`` URL.
    """
    text_source = str(source)
    if not mirroring_enabled():
        yield text_source
        return
    if text_source.startswith(("https://", "http://")):
        with urllib.request.urlopen(text_source, timeout=60) as response:  # noqa: S310 - fixed upstream URLs
            text = response.read().decode("utf-8")
    else:
        text = Path(text_source).read_text(encoding="utf-8")
    directory = tempfile.mkdtemp(prefix="sregym-mirrored-")
    try:
        path = Path(directory) / "manifest.yaml"
        path.write_text(rewrite_manifest_text(text), encoding="utf-8")
        yield str(path)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def helm_post_renderer_args() -> list[str]:
    """Extra ``helm install`` arguments that pipe the rendered chart through this module; empty when off."""
    if not mirroring_enabled():
        return []
    root = Path(__file__).resolve().parents[2]
    script = (
        f"import sys; sys.path.insert(0, {str(root)!r}); "
        "from sregym.service.image_mirror import main; raise SystemExit(main())"
    )
    return ["--post-renderer", sys.executable, "--post-renderer-args", "-c", "--post-renderer-args", script]


def main() -> int:
    sys.stdout.write(rewrite_manifest_text(sys.stdin.read()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
