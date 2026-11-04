"""App-neutral component drivers: one module per datastore/runtime kind.

Each driver exposes inject, reference-fix, decoy and check primitives that take a binding row
from ``sregym/conductor/problems/composition/app_bindings.py`` (never an app name). ``kube`` is
the one Kubernetes seam; ``http`` does entry-point fetches and the seeded-data snapshot.
See docs/incident-faults.md.
"""
