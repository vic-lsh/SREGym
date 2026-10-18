"""Fault families whose injections can be composed on distinct services."""

from __future__ import annotations

import random
from collections.abc import Callable

from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint

# Curated application services with both a Deployment and a Service. A draw
# claims its service name, so two faults cannot target the same service.
SERVICES = {
    "hotel_reservation": ("frontend", "geo", "profile", "rate", "recommendation", "search", "reservation", "user"),
    "social_network": (
        "compose-post-service",
        "home-timeline-service",
        "media-service",
        "post-storage-service",
        "social-graph-service",
        "text-service",
        "url-shorten-service",
        "user-service",
    ),
    "astronomy_shop": ("ad", "cart", "checkout", "currency", "frontend", "payment", "recommendation", "shipping"),
    "train_ticket": (
        "ts-auth-service",
        "ts-order-service",
        "ts-route-service",
        "ts-seat-service",
        "ts-ticketinfo-service",
        "ts-travel-service",
        "ts-user-service",
        "ts-station-service",
    ),
}


def _sample(key: str, kind: str, field: str) -> Callable[[str, random.Random], FaultDraw]:
    def sample(app: str, rng: random.Random) -> FaultDraw:
        service = rng.choice(SERVICES[app])
        return FaultDraw(
            key=key,
            app=app,
            params=(("faulty_service", service),),
            footprints=(Footprint(kind, service, field),),
            exclusive_tags=frozenset({f"service:{service}"}),
        )

    return sample


def _build(key: str, draw: FaultDraw):
    params = {"app_name": draw.app, **draw.param_dict()}
    if key == "target_port":
        from sregym.conductor.problems.target_port import K8STargetPortMisconfig

        return K8STargetPortMisconfig(**params)
    if key == "incorrect_image":
        from sregym.conductor.problems.incorrect_image import IncorrectImage

        return IncorrectImage(**params)
    if key == "readiness_probe":
        from sregym.conductor.problems.readiness_probe_misconfiguration import ReadinessProbeMisconfiguration

        return ReadinessProbeMisconfiguration(**params)
    raise ValueError(f"unknown composition family {key!r}")


_FAMILIES = (
    ("target_port", "Service", "spec.ports.targetPort"),
    ("incorrect_image", "Deployment", "spec.template.spec.containers.image"),
    ("readiness_probe", "Deployment", "spec.template.spec.containers.readinessProbe"),
)


def entries() -> tuple[CatalogEntry, ...]:
    """Return the deterministic catalog of supported fault families."""
    return tuple(
        CatalogEntry(
            key=key,
            apps=frozenset(SERVICES),
            sample=_sample(key, kind, field),
            build=lambda draw, key=key: _build(key, draw),
        )
        for key, kind, field in _FAMILIES
    )


def entries_for(app: str) -> tuple[CatalogEntry, ...]:
    if app not in SERVICES:
        raise ValueError(f"unsupported composition app {app!r}; expected one of {sorted(SERVICES)}")
    return entries()
