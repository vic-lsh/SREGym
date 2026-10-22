# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The multi-fault catalog: every composable fault class, discovered from family modules.

Each public submodule of this package defines `ENTRIES: tuple[CatalogEntry, ...]`. A new
family plugs in by adding a module; nothing here changes. Modules whose name starts with
"_" are private helpers and are skipped.
"""

from __future__ import annotations

import functools
import importlib
import pkgutil

from sregym.conductor.problems.composition.spec import CatalogEntry


@functools.cache
def entries() -> tuple[CatalogEntry, ...]:
    """All catalog entries, sorted by key. Raises ValueError on a malformed module or duplicate key."""
    owner: dict[str, str] = {}
    found: list[CatalogEntry] = []
    for module_info in sorted(pkgutil.iter_modules(__path__), key=lambda m: m.name):
        if module_info.name.startswith("_"):
            continue
        module_name = f"{__name__}.{module_info.name}"
        module = importlib.import_module(module_name)
        module_entries = getattr(module, "ENTRIES", None)
        if not isinstance(module_entries, tuple) or not all(isinstance(e, CatalogEntry) for e in module_entries):
            raise ValueError(f"{module_name} must define ENTRIES: tuple[CatalogEntry, ...]")
        for entry in module_entries:
            if entry.key in owner:
                raise ValueError(f"duplicate catalog key {entry.key!r} in {owner[entry.key]} and {module_name}")
            owner[entry.key] = module_name
            found.append(entry)
    return tuple(sorted(found, key=lambda e: e.key))


def entries_for(app: str) -> tuple[CatalogEntry, ...]:
    """Entries that support `app`, sorted by key."""
    return tuple(e for e in entries() if app in e.apps)


def apps() -> frozenset[str]:
    """Every app supported by at least one entry."""
    return frozenset(app for e in entries() for app in e.apps)
