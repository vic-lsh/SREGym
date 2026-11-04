"""Offline statistics over generated multi-fault plans (no cluster).

    uv run python tools/fault_validation/compose_stats.py --app social_network --generation 2 \
        --n 2 3 --seeds 2000 [--reliable] [--only-keys KEY ...]

For every (n, seed) it replays the generator's rejection sampling (same rng stream as
``generator.draw_plan``, asserted equal) and reports: the distribution of drawn kind pairs and
triples, how many sampled draws the conflict rules rejected (and by which rule), and plans that
failed to draw. ``--audit`` lists every pair of generation-2 draws that the rules let through
although both touch the same store family (same store name in any footprint kind, a read
dependency, a flushed cache) so a reviewer can judge each class.
"""

from __future__ import annotations

import argparse
import collections
import itertools
import json
import random
import sys

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.generator import (
    _ATTEMPTS_PER_FAULT,
    _RESTARTS,
    PlanError,
    _plan_rng,
    draw_plan,
)
from sregym.conductor.problems.composition.reliability import reliable_entries
from sregym.conductor.problems.composition.spec import conflicts


def rule_of(a, b) -> str:
    if a.exclusive_tags & b.exclusive_tags:
        return "exclusive_tag"
    if (a.needs_client_pods and b.tier == 2) or (b.needs_client_pods and a.tier == 2):
        return "client_pods_vs_admission"
    if a.may_restart and any(f.name in b.reads for f in a.footprints):
        return "restart_vs_reads"
    if b.may_restart and any(f.name in a.reads for f in b.footprints):
        return "restart_vs_reads"
    if any(fa.overlaps(fb) for fa in a.footprints for fb in b.footprints):
        return "footprint_overlap"
    return ""


def replay(app, n, seed, candidates, stats):
    rng = _plan_rng(app, n, seed)
    budget = _ATTEMPTS_PER_FAULT * n
    for _ in range(_RESTARTS):
        accepted = []
        for _ in range(budget):
            entry = rng.choice(candidates)
            draw = entry.sample(app, rng)
            stats["sampled"] += 1
            why = next((r for r in (rule_of(draw, o) for o in accepted) if r), "")
            if why:
                stats["rejected"] += 1
                stats["rejected_by"][why] += 1
                continue
            accepted.append(draw)
            if len(accepted) == n:
                return sorted(accepted, key=lambda d: d.tier)  # order only for display
    return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--app", default="social_network")
    ap.add_argument("--generation", type=int, default=2)
    ap.add_argument("--n", type=int, nargs="+", default=[2, 3])
    ap.add_argument("--seeds", type=int, default=2000)
    ap.add_argument("--reliable", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    entries = [e for e in catalog.entries_for(args.app) if e.generation(args.app) <= args.generation]
    if args.reliable:
        entries = list(reliable_entries(args.app, entries))
    candidates = sorted(entries, key=lambda e: e.key)
    new = {e.key for e in candidates if e.generation(args.app) == 2}
    out = {"entries": len(candidates), "gen2_keys": sorted(new)}
    for n in args.n:
        stats = {"sampled": 0, "rejected": 0, "rejected_by": collections.Counter(), "failed": 0}
        combos = collections.Counter()
        with_new = collections.Counter()
        for seed in range(args.seeds):
            plan = replay(args.app, n, seed, candidates, stats)
            if plan is None:
                stats["failed"] += 1
                continue
            real = draw_plan(args.app, n, seed, entries)
            assert sorted(d.key for d in real.draws) == sorted(d.key for d in plan), (n, seed)
            combo = tuple(sorted(d.key for d in plan))
            combos[combo] += 1
            with_new[sum(d.key in new for d in plan)] += 1
        out[f"n{n}"] = {
            **{k: (dict(v) if isinstance(v, collections.Counter) else v) for k, v in stats.items()},
            "plans_by_new_kind_count": dict(sorted(with_new.items())),
            "top_combos": [[list(c), k] for c, k in combos.most_common(15)],
            "distinct_combos": len(combos),
        }
        newonly = collections.Counter()
        for c, k in combos.items():
            if all(x in new for x in c):
                newonly[c] = k
        out[f"n{n}"]["all_new_combos"] = [[list(c), k] for c, k in newonly.most_common()]
    print(json.dumps(out, indent=1) if args.json else json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
