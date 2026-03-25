"""Variant specifications for all parameterizable problem classes.

Each VariantSpec declares the parameter space for a problem class.
The generate_all_variants() function computes the Cartesian product of
all dimensions, filters by constraints, and produces factory callables.
"""

from sregym.conductor.problems.variant_generator import VariantDimension, VariantSpec
from sregym.conductor.problems.variant_utils import SERVICES_BY_APP

# Import all problem classes that have variant specs
from sregym.conductor.problems.duplicate_pvc_mounts import DuplicatePVCMounts
from sregym.conductor.problems.incorrect_port_assignment import IncorrectPortAssignment
from sregym.conductor.problems.liveness_probe_misconfiguration import LivenessProbeMisconfiguration
from sregym.conductor.problems.liveness_probe_too_aggressive import LivenessProbeTooAggressive
from sregym.conductor.problems.missing_configmap import MissingConfigMap
from sregym.conductor.problems.missing_env_variable import MissingEnvVariable
from sregym.conductor.problems.missing_service import MissingService
from sregym.conductor.problems.readiness_probe_misconfiguration import ReadinessProbeMisconfiguration
from sregym.conductor.problems.rolling_update_misconfigured import RollingUpdateMisconfigured
from sregym.conductor.problems.service_dns_resolution_failure import ServiceDNSResolutionFailure
from sregym.conductor.problems.service_port_conflict import ServicePortConflict
from sregym.conductor.problems.sidecar_port_conflict import SidecarPortConflict
from sregym.conductor.problems.stale_coredns_config import StaleCoreDNSConfig
from sregym.conductor.problems.target_port import K8STargetPortMisconfig
from sregym.conductor.problems.wrong_dns_policy import WrongDNSPolicy
from sregym.conductor.problems.wrong_service_selector import WrongServiceSelector


def _service_in_app(params: dict) -> bool:
    """Constraint: faulty_service must be a known service in the given app."""
    return params["faulty_service"] in SERVICES_BY_APP.get(params["app_name"], [])


# All three standard apps
_STANDARD_APPS = ["social_network", "hotel_reservation", "astronomy_shop"]

# Representative non-database services per app for general fault injection.
# We pick a subset to avoid excessive variants while maintaining coverage.
_REPRESENTATIVE_SERVICES = {
    "social_network": [
        "user-service", "media-service", "compose-post-service",
        "post-storage-service", "social-graph-service", "url-shorten-service",
        "home-timeline-service", "text-service", "nginx-thrift",
    ],
    "hotel_reservation": [
        "frontend", "geo", "profile", "rate", "recommendation",
        "search", "reservation", "user",
    ],
    "astronomy_shop": [
        "ad", "cart", "checkout", "currency", "email", "frontend",
        "payment", "product-catalog", "recommendation", "shipping",
    ],
}

# All representative services flattened (for single-app problems)
_ALL_REPRESENTATIVE = sorted(set(
    svc for svcs in _REPRESENTATIVE_SERVICES.values() for svc in svcs
))

# Social network services only (for K8STargetPortMisconfig which only supports SocialNetwork)
_SOCIAL_NETWORK_SERVICES = _REPRESENTATIVE_SERVICES["social_network"]


def _representative_service_in_app(params: dict) -> bool:
    """Constraint: service must be in the representative set for the app."""
    app = params["app_name"]
    svc = params["faulty_service"]
    return svc in _REPRESENTATIVE_SERVICES.get(app, [])


# Env var -> value mappings for AstronomyShop services
_ASTRONOMY_SHOP_ENV_VARS = {
    "CART_ADDR": "cart:8080",
    "PRODUCT_CATALOG_ADDR": "product-catalog:8080",
    "CURRENCY_ADDR": "currency:8080",
    "SHIPPING_ADDR": "shipping:8080",
    "CHECKOUT_ADDR": "checkout:8080",
    "AD_ADDR": "ad:8080",
    "RECOMMENDATION_ADDR": "recommendation:8080",
}

# Which address env vars each astronomy-shop service actually has
# (derived from the Helm values.yaml)
_SERVICE_ENV_VARS: dict[str, set[str]] = {
    "checkout": {"CART_ADDR", "CURRENCY_ADDR", "SHIPPING_ADDR", "PRODUCT_CATALOG_ADDR"},
    "frontend": set(_ASTRONOMY_SHOP_ENV_VARS.keys()),
    "recommendation": {"PRODUCT_CATALOG_ADDR"},
}


VARIANT_SPECS: list[VariantSpec] = [
    # --- Probe Misconfigurations ---
    VariantSpec(
        problem_class=ReadinessProbeMisconfiguration,
        base_name="readiness_probe_misconfiguration",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=LivenessProbeMisconfiguration,
        base_name="liveness_probe_misconfiguration",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=LivenessProbeTooAggressive,
        base_name="liveness_probe_too_aggressive",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
        ],
    ),

    # --- Service Selector / DNS ---
    VariantSpec(
        problem_class=WrongServiceSelector,
        base_name="wrong_service_selector",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=WrongDNSPolicy,
        base_name="wrong_dns_policy",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=ServiceDNSResolutionFailure,
        base_name="service_dns_resolution_failure",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),

    # --- Missing Resources ---
    VariantSpec(
        problem_class=MissingService,
        base_name="missing_service",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=MissingConfigMap,
        base_name="missing_configmap",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),

    # --- Storage ---
    VariantSpec(
        problem_class=DuplicatePVCMounts,
        base_name="duplicate_pvc_mounts",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),

    # --- Port Conflicts ---
    VariantSpec(
        problem_class=SidecarPortConflict,
        base_name="sidecar_port_conflict",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=ServicePortConflict,
        base_name="service_port_conflict",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
        ],
        constraints=_representative_service_in_app,
    ),

    # --- Port Assignment / Target Port ---
    VariantSpec(
        problem_class=IncorrectPortAssignment,
        base_name="incorrect_port_assignment",
        dimensions=[
            VariantDimension("app_name", ["astronomy_shop"]),
            VariantDimension("faulty_service", list(_SERVICE_ENV_VARS.keys())),
            VariantDimension("env_var", list(_ASTRONOMY_SHOP_ENV_VARS.keys())),
            VariantDimension("incorrect_port", ["8082", "9090", "3000", "5432", "6379"]),
        ],
        constraints=lambda p: p["env_var"] in _SERVICE_ENV_VARS.get(p["faulty_service"], set()),
    ),
    VariantSpec(
        problem_class=K8STargetPortMisconfig,
        base_name="k8s_target_port_misconfig",
        dimensions=[
            VariantDimension("faulty_service", _SOCIAL_NETWORK_SERVICES),
            VariantDimension("bad_port", [9999, 8888, 7777, 6666]),
        ],
    ),

    # --- Rolling Update / CoreDNS ---
    VariantSpec(
        problem_class=RollingUpdateMisconfigured,
        base_name="rolling_update_misconfigured",
        dimensions=[
            VariantDimension("app_name", ["social_network", "hotel_reservation"]),
        ],
    ),
    VariantSpec(
        problem_class=StaleCoreDNSConfig,
        base_name="stale_coredns_config",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
        ],
    ),

    # --- Missing Env Variable ---
    VariantSpec(
        problem_class=MissingEnvVariable,
        base_name="missing_env_variable",
        dimensions=[
            VariantDimension("app_name", ["astronomy_shop"]),
            VariantDimension("faulty_service", ["frontend"]),
            VariantDimension("env_var", list(_ASTRONOMY_SHOP_ENV_VARS.keys())),
        ],
        constraints=lambda p: True,  # all combos valid for astronomy_shop frontend
        derived_params=lambda p: {"env_var_value": _ASTRONOMY_SHOP_ENV_VARS[p["env_var"]]},
    ),
]


def get_all_variant_specs() -> list[VariantSpec]:
    """Return all registered variant specifications."""
    return VARIANT_SPECS
