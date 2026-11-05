"""Scope of a single-record data fault: how much of a keyspace it corrupts.

A real incident (a bad deploy, a botched migration, broken invalidation) damages many records, not the one
that SREGym's own probe user reads. A scoped fault picks its records from the keys the app actually holds:

* ``wide``: every eligible key;
* ``partial``: a seeded random fraction of them;
* ``single``: one fixed key (the original behaviour, kept for old plan ids and an explicit opt-in).

Pure and deterministic: the same (keys, scope, fraction, seed) always selects the same records.
"""

from __future__ import annotations

import random
from collections.abc import Sequence

SCOPES = ("single", "partial", "wide")
SCOPED = ("partial", "wide")  # what a catalog generation >= 3 draws unless single scope is opted into
PARTIAL_FRACTIONS = (0.25, 0.5)
MIN_PARTIAL_KEYS = 2


def validate(scope: str, fraction: float | None) -> None:
    if scope not in SCOPES:
        raise ValueError(f"unknown scope {scope!r}; expected one of {SCOPES}")
    if scope == "partial":
        if fraction is None or not 0 < fraction < 1:
            raise ValueError(f"a partial scope needs a fraction in (0, 1), got {fraction!r}")
    elif fraction is not None:
        raise ValueError(f"a {scope} scope takes no fraction")


def select(keys: Sequence[str], scope: str, fraction: float | None, seed: int) -> tuple[str, ...]:
    """The keys a ``wide`` or ``partial`` fault corrupts, in sorted order. ``single`` is not selected here."""
    validate(scope, fraction)
    if scope == "single":
        raise ValueError("single scope names its key; nothing to select")
    pool = sorted(set(keys))
    if scope == "wide":
        return tuple(pool)
    assert fraction is not None
    count = max(MIN_PARTIAL_KEYS, round(len(pool) * fraction))
    if len(pool) <= count:
        raise ValueError(f"a partial scope needs more than {count} eligible keys, found {len(pool)}")
    return tuple(sorted(random.Random(seed).sample(pool, count)))


def spread(selected: Sequence[str], count: int) -> tuple[str, ...]:
    """Up to ``count`` of the selected keys, evenly spread (the verdict's sampled reads cover the whole selection)."""
    if count < 1 or not selected:
        return ()
    step = max(1, len(selected) // count)
    return tuple(selected[::step][:count])
