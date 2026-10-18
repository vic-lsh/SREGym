"""Resolve reproducible multi-fault problem IDs through the existing composite lifecycle."""

from __future__ import annotations

from sregym.conductor.problems.composition.generator import PLAN_ID_PREFIX, draw_plan, parse_plan_id, plan_id


def build_composite(plan):
    from sregym.conductor.problems.composite import ComponentSpec, CompositeFaultProblem, CompositeSpec
    from sregym.conductor.problems.composition.catalog import entries_for

    catalog = {entry.key: entry for entry in entries_for(plan.app)}
    components = []
    built = {}
    for index, draw in enumerate(plan.draws):
        name = f"{index}-{draw.key}-{draw.param_dict()['faulty_service']}"
        components.append(ComponentSpec(role="fault", problem=name))
        built[name] = catalog[draw.key].build(draw)
    spec = CompositeSpec(problem_id=plan_id(plan.app, plan.n, plan.seed), components=tuple(components))
    composite = CompositeFaultProblem(spec, component_factory=built.__getitem__)
    composite.fault_draws = plan.draws
    return composite


def resolve(problem_id: str):
    if not problem_id.startswith(PLAN_ID_PREFIX):
        return None
    parsed = parse_plan_id(problem_id)
    if parsed is None:
        raise ValueError(f"malformed multi-fault problem ID {problem_id!r}")
    from sregym.conductor.problems.composition.catalog import entries_for

    app, count, seed = parsed
    return build_composite(draw_plan(app, count, seed, entries_for(app)))
