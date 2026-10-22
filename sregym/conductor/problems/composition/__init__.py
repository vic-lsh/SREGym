"""Resolve reproducible multi-fault problem IDs through the existing composite lifecycle."""

from __future__ import annotations

from sregym.conductor.problems.composition.generator import PLAN_ID_PREFIX, parse_plan_id, plan_for, plan_id


def build_composite(plan):
    """Wrap a plan's drawn problems in one CompositeFaultProblem.

    Besides `fault_draws`, the composite records the plan's identity so faults can be matched
    across take-K subsets of one base plan: `composite_base_n` (the base plan's N),
    `composite_take_k` (K, or N for a whole plan) and `fault_base_index` (each draw's position in
    the base plan). Component names start with that base position.
    """
    from sregym.conductor.problems.composite import ComponentSpec, CompositeFaultProblem, CompositeSpec
    from sregym.conductor.problems.composition.catalog import entries_for

    catalog = {entry.key: entry for entry in entries_for(plan.app)}
    components = []
    built = {}
    for base, draw in zip(plan.base_index, plan.draws, strict=True):
        name = f"{base}-{draw.key}-{draw.param_dict()['faulty_service']}"
        components.append(ComponentSpec(role="fault", problem=name))
        built[name] = catalog[draw.key].build(draw)
    spec = CompositeSpec(problem_id=plan_id(plan.app, plan.n, plan.seed, plan.take), components=tuple(components))
    composite = CompositeFaultProblem(spec, component_factory=built.__getitem__)
    composite.fault_draws = plan.draws
    composite.composite_base_n = plan.n
    composite.composite_take_k = plan.n if plan.take is None else plan.take
    composite.fault_base_index = plan.base_index
    return composite


def resolve(problem_id: str):
    if not problem_id.startswith(PLAN_ID_PREFIX):
        return None
    ref = parse_plan_id(problem_id)
    if ref is None:
        raise ValueError(
            f"malformed multi-fault problem ID {problem_id!r}; expected "
            f"{PLAN_ID_PREFIX}<app>__n<N>__s<seed>[__k<K>]__v_ with 1 <= K <= N"
        )
    from sregym.conductor.problems.composition.catalog import entries_for

    return build_composite(plan_for(ref, entries_for(ref.app)))
