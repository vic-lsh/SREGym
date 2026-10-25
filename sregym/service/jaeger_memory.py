"""Bound ephemeral all-in-one Jaeger at the rendered-manifest boundary.

Shared by plain YAML, source overlays and Helm post-rendering, including chart
subdependencies. Persistent storage backends retain their own configuration.
"""

from __future__ import annotations

import contextlib
import copy
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

MAX_TRACES = 10_000
MEMORY_LIMIT = "2Gi"
MEMORY_REQUEST = "256Mi"


def bound_document(document: dict[str, Any] | None) -> None:
    if not document:
        return
    if document.get("kind") == "List":
        for item in document.get("items", []):
            bound_document(item)
        return
    spec = document.get("spec", {})
    if document.get("kind") == "CronJob":
        spec = spec.get("jobTemplate", {}).get("spec", {})
    pod = spec if document.get("kind") == "Pod" else spec.get("template", {}).get("spec", {})
    for container in pod.get("containers", []) + pod.get("initContainers", []):
        image = container.get("image", "").split("@", 1)[0].split(":", 1)[0]
        if image.rsplit("/", 1)[-1] != "all-in-one" or "jaegertracing/" not in image:
            continue
        environment = {row["name"]: row.get("value") for row in container.get("env", [])}
        if environment.get("SPAN_STORAGE_TYPE", "memory") != "memory":
            continue
        args = iter(container.get("args", []))
        kept = []
        for arg in args:
            if arg == "--memory.max-traces":
                next(args, None)
            elif not arg.startswith("--memory.max-traces="):
                kept.append(arg)
        container["args"] = kept + [f"--memory.max-traces={MAX_TRACES}"]
        container["env"] = [row for row in container.get("env", []) if row["name"] != "MEMORY_MAX_TRACES"]
        resources = container.setdefault("resources", {})
        resources.setdefault("limits", {})["memory"] = MEMORY_LIMIT
        resources.setdefault("requests", {})["memory"] = MEMORY_REQUEST


def render(text: str) -> str:
    documents = list(yaml.safe_load_all(text))
    for document in documents:
        bound_document(document)
    return yaml.safe_dump_all(documents, sort_keys=False)


def bound_directory(directory: Path) -> None:
    for path in sorted(directory.rglob("*")):
        if path.suffix.lower() not in {".yaml", ".yml", ".json"} or not path.is_file():
            continue
        original = list(yaml.safe_load_all(path.read_text()))
        documents = copy.deepcopy(original)
        for document in documents:
            if isinstance(document, dict):
                bound_document(document)
        if documents != original:
            path.write_text(yaml.safe_dump_all(documents, sort_keys=False))


@contextlib.contextmanager
def bounded_manifests(source: str | Path):
    """Render a private copy; never rewrite the pinned or accepted source tree."""
    with tempfile.TemporaryDirectory(prefix="sregym-jaeger-") as temporary:
        target = Path(temporary) / "manifests"
        source = Path(source)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            target.mkdir()
            shutil.copy2(source, target / source.name)
        bound_directory(target)
        yield target


def post_renderer_args() -> list[str]:
    return ["--post-renderer", sys.executable, "--post-renderer-args", str(Path(__file__).resolve())]


if __name__ == "__main__":
    sys.stdout.write(render(sys.stdin.read()))
