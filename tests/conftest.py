"""Pytest bootstrap for the sregym test suite.

The sregym package is laid out so that top-level helper packages such as
``llm_backend`` and ``clients`` sit alongside the ``sregym`` package itself.
When pytest is invoked it does not automatically add the project root to
``sys.path``, so importing modules whose dependency chain pulls in those
helpers (e.g. ``sregym.conductor.oracles.diagnosis_oracle`` triggers the
conductor ``__init__`` which transitively imports ``llm_backend``) fails
during collection.

Add the bench/sregym directory to ``sys.path`` once, at collection time, so
those imports resolve regardless of how pytest was launched.
"""

from __future__ import annotations

import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))
