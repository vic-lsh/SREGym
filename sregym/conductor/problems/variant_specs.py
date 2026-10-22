"""Variant specifications for all parameterizable problem classes.

Each VariantSpec declares the parameter space for a problem class.
The generate_all_variants() function computes the Cartesian product of
all dimensions, filters by constraints, and produces factory callables.
"""

# Import all problem classes that have variant specs
from sregym.conductor.problems.ad_service_manual_gc import AdServiceManualGc
from sregym.conductor.problems.admission_webhook_outage import AdmissionWebhookOutage
from sregym.conductor.problems.admission_webhook_tls_mismatch import AdmissionWebhookTLSMismatch
from sregym.conductor.problems.auth_miss_mongodb import MongoDBAuthMissing
from sregym.conductor.problems.cart_service_failure import CartServiceFailure
from sregym.conductor.problems.configmap_drift import ConfigMapDrift
from sregym.conductor.problems.duplicate_pvc_mounts import DuplicatePVCMounts
from sregym.conductor.problems.env_variable_shadowing import EnvVariableShadowing
from sregym.conductor.problems.ephemeral_port_range_hotel_reservation import EphemeralPortRangeHotelReservation
from sregym.conductor.problems.file_descriptor_exhaustion import FileDescriptorExhaustion
from sregym.conductor.problems.finalizer_deadlock_controller import FinalizerDeadlockController
from sregym.conductor.problems.incorrect_image import IncorrectImage
from sregym.conductor.problems.incorrect_port_assignment import IncorrectPortAssignment
from sregym.conductor.problems.init_container_dependency_hang import InitContainerDependencyHang
from sregym.conductor.problems.khaos_faults import (
    _HW_CPU_CLOCKSOURCE_FAILURE,
    _HW_DRAM_MODULE_FAILURE,
    _HW_NIC_PACKET_CORRUPTION,
    _HW_STORAGE_READ_FAILURE,
    _HW_STORAGE_WRITE_FAILURE,
    KhaosCompoundFaultProblem,
    KhaosFaultName,
    KhaosFaultProblem,
)
from sregym.conductor.problems.liveness_probe_misconfiguration import LivenessProbeMisconfiguration
from sregym.conductor.problems.liveness_probe_too_aggressive import LivenessProbeTooAggressive
from sregym.conductor.problems.missing_configmap import MissingConfigMap
from sregym.conductor.problems.missing_env_variable import MissingEnvVariable
from sregym.conductor.problems.missing_service import MissingService
from sregym.conductor.problems.network_policy_block import NetworkPolicyBlock
from sregym.conductor.problems.pod_anti_affinity_deadlock import PodAntiAffinityDeadlock
from sregym.conductor.problems.psa_restricted_blocks_recreation import PSARestrictedBlocksRecreation
from sregym.conductor.problems.pvc_claim_mismatch import PVCClaimMismatch
from sregym.conductor.problems.readiness_probe_misconfiguration import ReadinessProbeMisconfiguration
from sregym.conductor.problems.resource_request import ResourceRequestTooLarge, ResourceRequestTooSmall
from sregym.conductor.problems.revoke_auth import MongoDBRevokeAuth
from sregym.conductor.problems.rolling_update_misconfigured import RollingUpdateMisconfigured
from sregym.conductor.problems.service_dns_resolution_failure import ServiceDNSResolutionFailure
from sregym.conductor.problems.service_port_conflict import ServicePortConflict
from sregym.conductor.problems.sidecar_port_conflict import SidecarPortConflict
from sregym.conductor.problems.silent_data_corruption import SilentDataCorruption
from sregym.conductor.problems.stale_coredns_config import StaleCoreDNSConfig
from sregym.conductor.problems.storage_user_unregistered import MongoDBUserUnregistered
from sregym.conductor.problems.taint_no_toleration import TaintNoToleration
from sregym.conductor.problems.target_port import K8STargetPortMisconfig
from sregym.conductor.problems.train_ticket_f22 import TrainTicketF22
from sregym.conductor.problems.trainticket_f17 import TrainTicketF17
from sregym.conductor.problems.valkey_memory_disruption import ValkeyMemoryDisruption
from sregym.conductor.problems.variant_generator import VariantDimension, VariantSpec
from sregym.conductor.problems.variant_utils import SERVICES_BY_APP
from sregym.conductor.problems.wrong_dns_policy import WrongDNSPolicy
from sregym.conductor.problems.wrong_service_selector import WrongServiceSelector


def _service_in_app(params: dict) -> bool:
    """Constraint: faulty_service must be a known service in the given app."""
    return params["faulty_service"] in SERVICES_BY_APP.get(params["app_name"], [])


# All three standard apps
_STANDARD_APPS = ["social_network", "hotel_reservation", "astronomy_shop", "train_ticket"]

# Representative non-database services per app for general fault injection.
# We pick a subset to avoid excessive variants while maintaining coverage.
_REPRESENTATIVE_SERVICES = {
    "social_network": [
        "user-service",
        "media-service",
        "compose-post-service",
        "post-storage-service",
        "social-graph-service",
        "url-shorten-service",
        "home-timeline-service",
        "text-service",
        "nginx-thrift",
    ],
    "hotel_reservation": [
        "frontend",
        "geo",
        "profile",
        "rate",
        "recommendation",
        "search",
        "reservation",
        "user",
    ],
    "astronomy_shop": [
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "frontend",
        "payment",
        "product-catalog",
        "recommendation",
        "shipping",
    ],
    "train_ticket": [
        "ts-auth-service",
        "ts-order-service",
        "ts-route-service",
        "ts-seat-service",
        "ts-basic-service",
        "ts-travel-service",
        "ts-user-service",
        "ts-station-service",
    ],
}

# All representative services flattened (for single-app problems)
_ALL_REPRESENTATIVE = sorted(set(svc for svcs in _REPRESENTATIVE_SERVICES.values() for svc in svcs))

# Social network services for the existing target-port variant IDs.
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


_COMPOUND_HW_FAILURE_TYPES = {
    "nic_packet_corruption": {
        "fault_specs": [
            (KhaosFaultName.packet_loss_sendto, [30]),
            (KhaosFaultName.packet_loss_recvfrom, [30]),
        ],
        "root_cause": _HW_NIC_PACKET_CORRUPTION,
    },
    "storage_controller_read_failure": {
        "fault_specs": [
            (KhaosFaultName.read_error, None),
            (KhaosFaultName.pread_error, None),
        ],
        "root_cause": _HW_STORAGE_READ_FAILURE,
    },
    "storage_write_failure": {
        "fault_specs": [
            (KhaosFaultName.write_error, None),
            (KhaosFaultName.pwrite_error, None),
            (KhaosFaultName.fsync_error, None),
        ],
        "root_cause": _HW_STORAGE_WRITE_FAILURE,
    },
    "dram_module_failure": {
        "fault_specs": [
            (KhaosFaultName.mmap_fail, None),
            (KhaosFaultName.mmap_oom, None),
            (KhaosFaultName.oom_memchunk, None),
        ],
        "root_cause": _HW_DRAM_MODULE_FAILURE,
    },
    "cpu_clocksource_failure": {
        "fault_specs": [
            (KhaosFaultName.clock_drift, None),
            (KhaosFaultName.gettimeofday_fail, None),
        ],
        "root_cause": _HW_CPU_CLOCKSOURCE_FAILURE,
    },
}

_PROBE_ERROR_MODES = ["wrong_path", "wrong_port", "wrong_protocol", "exec_false"]

_REPLICA_COUNTS = [1, 3]

_RATE_PARAMETERIZED_FAULTS = frozenset(
    [
        KhaosFaultName.packet_loss_sendto,
        KhaosFaultName.packet_loss_recvfrom,
        KhaosFaultName.latent_sector_error,
    ]
)


def _wrong_selector_derived_params(params: dict) -> dict:
    """For swap_to_other_service mutation, pick a different service as the wrong target."""
    if params["selector_mutation"] == "swap_to_other_service":
        app = params["app_name"]
        svc = params["faulty_service"]
        candidates = [s for s in _REPRESENTATIVE_SERVICES[app] if s != svc]
        if candidates:
            return {"wrong_target_deployment": candidates[0]}
    return {}


VARIANT_SPECS: list[VariantSpec] = [
    # --- Probe Misconfigurations ---
    VariantSpec(
        problem_class=ReadinessProbeMisconfiguration,
        base_name="readiness_probe_misconfiguration",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("probe_error_mode", _PROBE_ERROR_MODES),
            VariantDimension("replica_count", _REPLICA_COUNTS),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=LivenessProbeMisconfiguration,
        base_name="liveness_probe_misconfiguration",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("probe_error_mode", _PROBE_ERROR_MODES),
            VariantDimension("replica_count", _REPLICA_COUNTS),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=LivenessProbeTooAggressive,
        base_name="liveness_probe_too_aggressive",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("aggression_profile", ["aggressive", "slow_churn"]),
            VariantDimension("startup_probe_guard", ["none", "short_startup"]),
        ],
        constraints=_representative_service_in_app,
    ),
    # --- Service Selector / DNS ---
    VariantSpec(
        problem_class=WrongServiceSelector,
        base_name="wrong_service_selector",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("replica_count", _REPLICA_COUNTS),
            # "swap_to_other_service" was removed: it repoints the Service to a
            # different (still-Ready) deployment's pods, which the attached
            # ServiceEndpointMitigationOracle (endpoint-presence only) can never
            # detect — the mitigation stage passes the instant the fault is
            # injected. Re-adding it needs an endpoint-ownership oracle.
            VariantDimension("selector_mutation", ["add_extra_label"]),
        ],
        constraints=_representative_service_in_app,
        derived_params=_wrong_selector_derived_params,
    ),
    VariantSpec(
        problem_class=WrongDNSPolicy,
        base_name="wrong_dns_policy",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("dns_misconfiguration_type", ["external_resolver", "default_policy"]),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=ServiceDNSResolutionFailure,
        base_name="service_dns_resolution_failure",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("dns_response_code", ["NXDOMAIN", "SERVFAIL"]),
            VariantDimension("dns_failure_layer", ["coredns_template", "network_policy_block_dns"]),
        ],
        # dns_response_code is only meaningful for the coredns_template layer. The
        # network_policy_block_dns layer injects a deny-all-egress NetworkPolicy that
        # is rcode-independent, so pin it to a single rcode (NXDOMAIN, the default) to
        # avoid emitting byte-identical NXDOMAIN/SERVFAIL duplicate variants. (Kept as
        # an inline predicate — variant_specs is loaded in isolation in tests with the
        # problem modules stubbed, so it must not import runtime constants from them.)
        constraints=lambda p: (
            _representative_service_in_app(p)
            and (p["dns_failure_layer"] == "coredns_template" or p["dns_response_code"] == "NXDOMAIN")
        ),
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
            VariantDimension("app_name", ["social_network", "hotel_reservation"]),
            # "clear_data" was removed: on both generated targets it is a no-op —
            # media-mongodb's only key already holds MongoDB's default, and hotel's
            # mongo-geo-script is an init-only ConfigMap that never re-runs on a
            # persistent PVC. In both cases pods stay Ready and the readiness-based
            # MitigationOracle passes with the fault "active". Re-adding it needs a
            # target whose emptied config actually breaks readiness + a functional oracle.
            VariantDimension("configmap_fault_mode", ["delete"]),
        ],
        derived_params=lambda p: {
            "faulty_service": {
                "social_network": "media-mongodb",
                "hotel_reservation": "mongodb-geo",
            }[p["app_name"]],
        },
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
    VariantSpec(
        problem_class=K8STargetPortMisconfig,
        base_name="k8s_target_port_misconfig_hotel_reservation",
        dimensions=[
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
            VariantDimension("bad_port", [9999]),
        ],
        derived_params=lambda _: {"app_name": "hotel_reservation"},
    ),
    # --- Rolling Update / CoreDNS ---
    VariantSpec(
        problem_class=RollingUpdateMisconfigured,
        base_name="rolling_update_misconfigured",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("init_hang_mode", ["sleep_infinity", "crash_loop", "slow_timeout"]),
            VariantDimension("strategy_profile", ["drain_all", "stuck_canary"]),
        ],
        constraints=_representative_service_in_app,
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
            VariantDimension("env_corruption_mode", ["remove", "empty_value"]),
        ],
        derived_params=lambda p: {"env_var_value": _ASTRONOMY_SHOP_ENV_VARS[p["env_var"]]},
    ),
    # Additional parameterized fault families.
    VariantSpec(
        problem_class=MongoDBAuthMissing,
        base_name="auth_miss_mongodb",
        dimensions=[
            VariantDimension("faulty_service", ["url-shorten-mongodb"]),
        ],
    ),
    VariantSpec(
        problem_class=MongoDBRevokeAuth,
        base_name="revoke_auth_mongodb",
        dimensions=[
            VariantDimension("faulty_service", ["mongodb-geo", "mongodb-rate"]),
        ],
    ),
    VariantSpec(
        problem_class=MongoDBUserUnregistered,
        base_name="storage_user_unregistered",
        dimensions=[
            VariantDimension("faulty_service", ["mongodb-geo", "mongodb-rate"]),
        ],
    ),
    VariantSpec(
        problem_class=FileDescriptorExhaustion,
        base_name="file_descriptor_exhaustion",
        dimensions=[
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
            VariantDimension("ulimit_severity", ["severe", "moderate"]),
        ],
    ),
    VariantSpec(
        problem_class=AdServiceManualGc,
        base_name="ad_service_manual_gc",
        dimensions=[
            VariantDimension("faulty_service", ["ad"]),
        ],
        derived_params=lambda p: {"feature_flag": f"{p['faulty_service']}ManualGc"},
    ),
    VariantSpec(
        problem_class=CartServiceFailure,
        base_name="cart_service_failure",
        dimensions=[
            VariantDimension("faulty_service", ["cart"]),
        ],
        derived_params=lambda p: {"feature_flag": f"{p['faulty_service']}Failure"},
    ),
    VariantSpec(
        problem_class=IncorrectImage,
        base_name="incorrect_image",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("bad_image_kind", ["nonexistent_tag", "nonexistent_registry", "wrong_base_image"]),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=InitContainerDependencyHang,
        base_name="init_container_dependency_hang",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("init_hang_mechanism", ["dns_lookup_loop", "tcp_connect_loop", "http_ready_check"]),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=AdmissionWebhookOutage,
        base_name="admission_webhook_outage",
        dimensions=[
            VariantDimension("app_name", ["hotel_reservation"]),
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
            VariantDimension("webhook_kind", ["validating", "mutating"]),
        ],
    ),
    VariantSpec(
        problem_class=AdmissionWebhookTLSMismatch,
        base_name="admission_webhook_tls_mismatch",
        dimensions=[
            VariantDimension("app_name", ["hotel_reservation"]),
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
            VariantDimension("webhook_kind", ["validating", "mutating"]),
        ],
    ),
    VariantSpec(
        problem_class=PSARestrictedBlocksRecreation,
        base_name="psa_restricted_blocks_recreation",
        dimensions=[
            VariantDimension("app_name", ["hotel_reservation"]),
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
        ],
    ),
    VariantSpec(
        problem_class=NetworkPolicyBlock,
        base_name="network_policy_block",
        dimensions=[
            VariantDimension("app_name", ["hotel_reservation"]),
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["hotel_reservation"]),
            VariantDimension("policy_direction", ["ingress_only", "egress_only", "both"]),
            VariantDimension("policy_name_style", ["descriptive", "opaque", "misleading"]),
        ],
    ),
    VariantSpec(
        problem_class=ConfigMapDrift,
        base_name="configmap_drift",
        dimensions=[
            VariantDimension("faulty_service", ["geo"]),
        ],
    ),
    VariantSpec(
        problem_class=EnvVariableShadowing,
        base_name="env_variable_shadowing",
        dimensions=[
            VariantDimension("faulty_service", ["frontend-proxy"]),
        ],
    ),
    VariantSpec(
        problem_class=KhaosCompoundFaultProblem,
        base_name="khaos_compound_hw_failure",
        dimensions=[
            VariantDimension("compound_hw_failure_type", sorted(_COMPOUND_HW_FAILURE_TYPES.keys())),
        ],
        derived_params=lambda p: _COMPOUND_HW_FAILURE_TYPES[p["compound_hw_failure_type"]],
    ),
    VariantSpec(
        problem_class=SilentDataCorruption,
        base_name="silent_data_corruption",
        dimensions=[
            VariantDimension("strategy", ["read_corrupt", "write_corrupt", "both_corrupt"]),
            VariantDimension("target_deploy", ["mongodb-geo", "mongodb-rate", "mongodb-profile"]),
            VariantDimension("probability", [10, 100]),
            VariantDimension("corruption_timing", ["always_on", "intermittent_short", "intermittent_long"]),
        ],
    ),
    VariantSpec(
        problem_class=ResourceRequestTooLarge,
        base_name="resource_request_too_large",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            VariantDimension("resource_type", ["memory", "cpu"]),
            # resource_target removed: scheduling only considers requests, so
            # requests_only and both yield the same Pending symptom (degenerate),
            # and limits_only was a semantic inversion (a too-LOW limit) duplicating
            # ResourceRequestTooSmall. The class default ("both") is used and now
            # sets requests==limits so the spec stays valid on apps with pre-declared
            # limits (astronomy_shop, train_ticket).
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=ResourceRequestTooSmall,
        base_name="resource_request_too_small",
        dimensions=[
            VariantDimension("app_name", _STANDARD_APPS),
            VariantDimension("faulty_service", _ALL_REPRESENTATIVE),
            # "cpu" removed: a 5m CPU limit only throttles the container; on the
            # DeathStarBench apps (social_network, hotel_reservation) whose
            # deployments render NO readiness/liveness probes, a throttled pod
            # stays Running+Ready, so the readiness-based MitigationOracle passes
            # with the fault active (degenerate). Memory (10Mi) OOMKills the pod
            # -> CrashLoopBackOff -> not Ready, which the oracle detects on every
            # app. Re-adding "cpu" needs a throttling/latency oracle (or scoping
            # it to probe-bearing apps with a verified not-Ready signal).
            # resource_target removed: limits_only either duplicates "both" (memory,
            # no baseline request) or produces requests>limits which the API rejects
            # (cpu on hotel_reservation's 100m baseline), leaving the deployment
            # absent instead of throttled. The class default ("both") lowers requests
            # and limits together and is valid everywhere.
            VariantDimension("resource_type", ["memory"]),
        ],
        constraints=_representative_service_in_app,
    ),
    VariantSpec(
        problem_class=KhaosFaultProblem,
        base_name="khaos_single_fault",
        dimensions=[
            VariantDimension(
                "fault_name",
                [
                    KhaosFaultName.read_error,
                    KhaosFaultName.pread_error,
                    KhaosFaultName.write_error,
                    KhaosFaultName.fsync_error,
                    KhaosFaultName.latent_sector_error,
                    KhaosFaultName.mmap_fail,
                    KhaosFaultName.oom_memchunk,
                    KhaosFaultName.socket_block,
                    KhaosFaultName.bind_enetdown,
                    KhaosFaultName.packet_loss_sendto,
                    KhaosFaultName.packet_loss_recvfrom,
                    KhaosFaultName.clock_drift,
                    KhaosFaultName.gettimeofday_fail,
                    KhaosFaultName.fork_fail,
                    KhaosFaultName.force_connect_econnrefused,
                    KhaosFaultName.force_open_enoent,
                    KhaosFaultName.force_openat_enoent,
                ],
            ),
            VariantDimension("fault_severity", ["low", "moderate", "severe"]),
        ],
        constraints=lambda p: p["fault_name"] in _RATE_PARAMETERIZED_FAULTS or p["fault_severity"] == "moderate",
    ),
    VariantSpec(
        problem_class=EphemeralPortRangeHotelReservation,
        base_name="ephemeral_port_range_hotel_reservation",
        dimensions=[
            VariantDimension("port_range_severity", ["critical", "degraded"]),
            VariantDimension("sysctl_scope", ["all_deployments", "frontend_only"]),
        ],
    ),
    VariantSpec(
        problem_class=TaintNoToleration,
        base_name="taint_no_toleration",
        dimensions=[
            # "no_execute" removed: this problem taints EVERY node, and a NoExecute
            # taint evicts all non-tolerating pods cluster-wide (other namespaces,
            # system add-ons, co-tenant benchmark workers) — an uncontrolled blast
            # radius that breaks isolation and contradicts component=user-service.
            VariantDimension("taint_effect", ["no_schedule"]),
        ],
    ),
    VariantSpec(
        problem_class=PodAntiAffinityDeadlock,
        base_name="pod_anti_affinity_deadlock",
        dimensions=[
            VariantDimension("faulty_service", _REPRESENTATIVE_SERVICES["social_network"]),
            VariantDimension("anti_affinity_topology", ["hostname", "zone"]),
        ],
    ),
    VariantSpec(
        problem_class=FinalizerDeadlockController,
        base_name="finalizer_deadlock_controller",
        dimensions=[
            VariantDimension(
                "rbac_break_mode",
                [
                    "remove_write_verbs",
                    "drop_subresource_rule",
                    "add_resourcenames_restriction",
                ],
            ),
        ],
    ),
    VariantSpec(
        problem_class=PVCClaimMismatch,
        base_name="pvc_claim_mismatch",
        dimensions=[
            VariantDimension("pvc_failure_scope", ["single", "all"]),
        ],
    ),
    VariantSpec(
        problem_class=ValkeyMemoryDisruption,
        base_name="valkey_memory_disruption",
        dimensions=[
            # "allkeys_lru" removed: the value was malformed (underscore form is
            # rejected by CONFIG SET maxmemory-policy) and degenerate — valkey-cart
            # has no maxmemory configured, so no eviction occurs and the flood just
            # OOMKills the pod, identical to "noeviction". A real allkeys-lru variant
            # needs a bounded maxmemory plus a data-integrity oracle.
            VariantDimension("memory_eviction_policy", ["noeviction"]),
        ],
    ),
    VariantSpec(
        problem_class=TrainTicketF17,
        base_name="trainticket_f17",
        dimensions=[
            VariantDimension("decoy_flag_count", ["none", "low", "high"]),
        ],
    ),
    VariantSpec(
        problem_class=TrainTicketF22,
        base_name="train_ticket_f22",
        dimensions=[
            VariantDimension("decoy_flag_count", ["none", "low", "high"]),
        ],
    ),
]


def get_all_variant_specs() -> list[VariantSpec]:
    """Return all registered variant specifications."""
    return VARIANT_SPECS
