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
"""Catalog family: application-layer, env/config, ingress and app-level object faults.

Covers the READY and FIXABLE rows of audit part 1 plus the app-level, non-admission rows of part 5
(`docs/robust-impl/notes/2026-10-composition-audit.md`). Excluded rows and the reason for each are
recorded in `docs/robust-impl/findings/sregym-composition.md` (Catalog and generator, app-level).

Tier rule: RESTARTS_PODS when the injector restarts pods outside its own declared footprint or is
health-gated on pods it does not own (audit §2 C3); ORDINARY otherwise.
"""

from __future__ import annotations

import hashlib
import random
from typing import Any

from sregym.conductor.problems.composition.catalog._kit import (
    ALL_APPS,
    HOTEL_DEPLOYMENTS,
    REPRESENTATIVE_SERVICES,
    Pick,
    container,
    deployment,
    entry,
    fixed,
)
from sregym.conductor.problems.composition.spec import CatalogEntry, Footprint, Tier

_HOTEL = ("hotel_reservation",)
_ASTRONOMY = ("astronomy_shop",)


def _image_and_replicas(services: tuple[str, ...]) -> tuple[Footprint, ...]:
    """`inject_incorrect_image` sets the image and forces replicas 0 -> 1 (inject_app.py:388-403)."""
    return tuple(fp for svc in services for fp in (container(svc, "image"), deployment(svc, "spec.replicas")))


# --- FaultyImageCorrelated / UpdateIncompatibleCorrelated (fixed target sets) ----------------------

_CORRELATED_SERVICES = ("frontend", "geo", "profile", "rate", "recommendation", "reservation", "user", "search")
_MONGO_SERVICES = (
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
    "mongodb-user",
)


def _build_faulty_image_correlated(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.faulty_image_correlated import FaultyImageCorrelated

    return FaultyImageCorrelated()


def _build_update_incompatible_correlated(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.update_incompatible_correlated import UpdateIncompatibleCorrelated

    return UpdateIncompatibleCorrelated()


# --- IncorrectImage ---------------------------------------------------------------------------------

_BAD_IMAGE_KINDS = ("nonexistent_tag", "nonexistent_registry", "wrong_base_image")


def _choose_incorrect_image(app: str, rng: random.Random) -> Pick:
    svc = rng.choice(REPRESENTATIVE_SERVICES[app])
    kind = rng.choice(_BAD_IMAGE_KINDS)
    return Pick({"faulty_service": svc, "bad_image_kind": kind}, _image_and_replicas((svc,)))


def _build_incorrect_image(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.incorrect_image import IncorrectImage

    return IncorrectImage(app_name=app, **params)


# --- IncorrectPortAssignment / MissingEnvVariable (astronomy env vars) -------------------------------

_ASTRONOMY_ENV_VARS = {
    "CART_ADDR": "cart:8080",
    "PRODUCT_CATALOG_ADDR": "product-catalog:8080",
    "CURRENCY_ADDR": "currency:8080",
    "SHIPPING_ADDR": "http://shipping:8080",
    "CHECKOUT_ADDR": "checkout:8080",
    "AD_ADDR": "ad:8080",
    "RECOMMENDATION_ADDR": "recommendation:8080",
}
_SERVICE_ENV_VARS = {
    "checkout": ("CART_ADDR", "CURRENCY_ADDR", "PRODUCT_CATALOG_ADDR", "SHIPPING_ADDR"),
    "frontend": tuple(sorted(_ASTRONOMY_ENV_VARS)),
    "recommendation": ("PRODUCT_CATALOG_ADDR",),
}
_INCORRECT_PORTS = ("8082", "9090", "3000", "5432", "6379")


def _choose_incorrect_port(app: str, rng: random.Random) -> Pick:
    svc = rng.choice(sorted(_SERVICE_ENV_VARS))
    env_var = rng.choice(_SERVICE_ENV_VARS[svc])
    port = rng.choice(_INCORRECT_PORTS)
    return Pick(
        {"faulty_service": svc, "env_var": env_var, "incorrect_port": port},
        (container(svc, f"env.{env_var}"),),
    )


def _build_incorrect_port(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.incorrect_port_assignment import IncorrectPortAssignment

    return IncorrectPortAssignment(app_name=app, **params)


def _choose_missing_env(app: str, rng: random.Random) -> Pick:
    env_var = rng.choice(sorted(_ASTRONOMY_ENV_VARS))
    mode = rng.choice(("remove", "empty_value"))
    return Pick(
        {
            "faulty_service": "frontend",
            "env_var": env_var,
            "env_var_value": _ASTRONOMY_ENV_VARS[env_var],
            "env_corruption_mode": mode,
        },
        (container("frontend", f"env.{env_var}"),),
    )


def _build_missing_env(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.missing_env_variable import MissingEnvVariable

    return MissingEnvVariable(app_name=app, **params)


# --- MisconfigApp / WrongBinUsage / ConfigMapDrift (single working target) --------------------------


def _build_misconfig_app(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.misconfig_app import MisconfigAppHotelRes

    return MisconfigAppHotelRes()


def _build_wrong_bin_usage(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.wrong_bin_usage import WrongBinUsage

    return WrongBinUsage(**params)


def _build_configmap_drift(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.configmap_drift import ConfigMapDrift

    return ConfigMapDrift(app_name=app, **params)


def _build_env_variable_shadowing(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.env_variable_shadowing import EnvVariableShadowing

    return EnvVariableShadowing(app_name=app, **params)


# --- Mongo auth faults (in-DB state on mongodb-geo / mongodb-rate) ----------------------------------

_MONGO_AUTH_TARGETS = ("mongodb-geo", "mongodb-rate")


def _choose_mongo_auth(app: str, rng: random.Random) -> Pick:
    # The in-DB principal lives in the mongo pod: whole-Deployment footprint, so RevokeAuth and
    # UserUnregistered on one mongo conflict, as does any fault that recreates that pod.
    svc = rng.choice(_MONGO_AUTH_TARGETS)
    return Pick({"faulty_service": svc}, (deployment(svc),))


def _build_revoke_auth(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.revoke_auth import MongoDBRevokeAuth

    return MongoDBRevokeAuth(app_name=app, **params)


def _build_user_unregistered(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.storage_user_unregistered import MongoDBUserUnregistered

    return MongoDBUserUnregistered(app_name=app, **params)


# --- Valkey ------------------------------------------------------------------------------------------


def _build_valkey_auth(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.valkey_auth_disruption import ValkeyAuthDisruption

    return ValkeyAuthDisruption()


def _build_valkey_memory(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.valkey_memory_disruption import ValkeyMemoryDisruption

    return ValkeyMemoryDisruption(**params)


# --- EdgeRequestFilterCPUSaturation ------------------------------------------------------------------


def _build_edge_request_filter(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.edge_request_filter_cpu_saturation import EdgeRequestFilterCPUSaturation

    return EdgeRequestFilterCPUSaturation()


# --- EphemeralPortRange ------------------------------------------------------------------------------

_SYSCTLS = "spec.template.spec.securityContext.sysctls"


def _choose_ephemeral_port_range(app: str, rng: random.Random) -> Pick:
    severity = rng.choice(("critical", "degraded"))
    scope = rng.choice(("all_deployments", "frontend_only"))
    targets = HOTEL_DEPLOYMENTS if scope == "all_deployments" else ("frontend",)
    # all_deployments rolls every pod in the namespace (audit §1 hazard 2): inject it first.
    tier = Tier.RESTARTS_PODS if scope == "all_deployments" else Tier.ORDINARY
    return Pick(
        {"port_range_severity": severity, "sysctl_scope": scope},
        tuple(deployment(svc, _SYSCTLS) for svc in targets),
        tier,
    )


def _build_ephemeral_port_range(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.ephemeral_port_range_hotel_reservation import (
        EphemeralPortRangeHotelReservation,
    )

    return EphemeralPortRangeHotelReservation(**params)


# --- SecretRotationStaleEnvCredentials ---------------------------------------------------------------


def _build_secret_rotation(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.secret_rotation_stale_env_credentials import (
        SecretRotationStaleEnvCredentialsAstronomyShop,
    )

    return SecretRotationStaleEnvCredentialsAstronomyShop()


# --- FileDescriptorExhaustion ------------------------------------------------------------------------


def _choose_fd_exhaustion(app: str, rng: random.Random) -> Pick:
    svc = rng.choice(REPRESENTATIVE_SERVICES["hotel_reservation"])
    severity = rng.choice(("severe", "moderate"))
    return Pick(
        {"faulty_service": svc, "ulimit_severity": severity},
        (container(svc, "command"), container(svc, "args")),
    )


def _build_fd_exhaustion(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.file_descriptor_exhaustion import FileDescriptorExhaustion

    return FileDescriptorExhaustion(**params)


# --- Part 5 app-level objects ------------------------------------------------------------------------


def _build_expired_tls(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.expired_tls_hotel_reservation import ExpiredTlsHotelReservation

    return ExpiredTlsHotelReservation()


def _build_ingress_misroute(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.ingress_misroute import IngressMisroute

    return IngressMisroute()


def _build_dev_shm(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.dev_shm_exhaustion_hotel_reservation import DevShmExhaustionHotelReservation

    return DevShmExhaustionHotelReservation()


def _build_internal_traffic_policy(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.internal_traffic_policy_local import InternalTrafficPolicyLocalAstronomyShop

    return InternalTrafficPolicyLocalAstronomyShop()


def _build_cronjob_sidecar(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.cronjob_sidecar_blocks_completion import (
        CronJobSidecarBlocksCompletionHotelReservation,
    )

    return CronJobSidecarBlocksCompletionHotelReservation()


# --- NetworkPolicyBlock ------------------------------------------------------------------------------

# social_network is left out: the class hard-codes the hotel wrk2 payload script onto its app
# (network_policy_block.py:53-55), which would drive the wrong workload if it were the composite's
# first sub-problem. astronomy_shop and train_ticket use Locust and ignore payload_script.
_NETWORK_POLICY_APPS = ("hotel_reservation", "astronomy_shop", "train_ticket")
_POLICY_DIRECTION_LABELS = {"ingress_only": "ingress-only", "egress_only": "egress-only", "both": "all"}


def _network_policy_name(style: str, direction: str, svc: str) -> str:
    """Mirror of `network_policy_block._POLICY_NAME_STYLES[style]["name_fn"]`."""
    label = _POLICY_DIRECTION_LABELS[direction]
    if style == "descriptive":
        return f"deny-{label}-{svc}"
    if style == "opaque":
        return f"np-ext-{hashlib.md5(svc.encode()).hexdigest()[:6]}"
    if style == "misleading":
        return f"allow-traffic-{svc}"
    raise ValueError(f"unknown policy_name_style {style!r}")


def _choose_network_policy(app: str, rng: random.Random) -> Pick:
    svc = rng.choice(REPRESENTATIVE_SERVICES[app])
    direction = rng.choice(sorted(_POLICY_DIRECTION_LABELS))
    style = rng.choice(("descriptive", "opaque", "misleading"))
    return Pick(
        {"faulty_service": svc, "policy_direction": direction, "policy_name_style": style},
        (Footprint("NetworkPolicy", _network_policy_name(style, direction, svc)),),
    )


def _build_network_policy(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.network_policy_block import NetworkPolicyBlock

    return NetworkPolicyBlock(app_name=app, **params)


ENTRIES: tuple[CatalogEntry, ...] = (
    entry(
        "faulty_image_correlated",
        _HOTEL,
        fixed(Pick({}, _image_and_replicas(_CORRELATED_SERVICES))),
        _build_faulty_image_correlated,
    ),
    entry(
        "update_incompatible_correlated",
        _HOTEL,
        fixed(Pick({}, _image_and_replicas(_MONGO_SERVICES))),
        _build_update_incompatible_correlated,
    ),
    entry("incorrect_image", ALL_APPS, _choose_incorrect_image, _build_incorrect_image),
    entry("incorrect_port_assignment", _ASTRONOMY, _choose_incorrect_port, _build_incorrect_port),
    entry(
        "misconfig_app_hotel_res",
        _HOTEL,
        fixed(Pick({}, (container("geo", "image"),))),
        _build_misconfig_app,
    ),
    entry("missing_env_variable", _ASTRONOMY, _choose_missing_env, _build_missing_env),
    entry("revoke_auth_mongodb", _HOTEL, _choose_mongo_auth, _build_revoke_auth),
    entry("storage_user_unregistered", _HOTEL, _choose_mongo_auth, _build_user_unregistered),
    # Runtime config of the valkey-cart process (CONFIG SET requirepass): a restart clears it.
    entry(
        "valkey_auth_disruption",
        _ASTRONOMY,
        fixed(Pick({}, (deployment("valkey-cart"),))),
        _build_valkey_auth,
    ),
    entry(
        "valkey_memory_disruption",
        _ASTRONOMY,
        fixed(
            Pick(
                {"memory_eviction_policy": "noeviction"},
                (deployment("valkey-cart"), Footprint("Job", "valkey-memory-flood")),
            )
        ),
        _build_valkey_memory,
    ),
    # Recovery is `kubectl rollout undo` on frontend-proxy (whole object); the traffic driver is a
    # process inside the load-generator pod.
    entry(
        "edge_request_filter_cpu_saturation",
        _ASTRONOMY,
        fixed(Pick({}, (deployment("frontend-proxy"), deployment("load-generator")))),
        _build_edge_request_filter,
    ),
    entry(
        "ephemeral_port_range_hotel_reservation",
        _HOTEL,
        _choose_ephemeral_port_range,
        _build_ephemeral_port_range,
    ),
    # Stale state is keyed by the product-catalog pod UID and the postgres role password: any
    # restart of either heals it, so both are whole-object footprints.
    entry(
        "secret_rotation_stale_env_credentials_astronomy_shop",
        _ASTRONOMY,
        fixed(
            Pick(
                {},
                (
                    deployment("product-catalog"),
                    deployment("postgresql"),
                    container("accounting", "env.DB_CONNECTION_STRING"),
                    container("product-reviews", "env.DB_CONNECTION_STRING"),
                    Footprint("Secret", "product-catalog-db-conn"),
                    Footprint("ConfigMap", "postgresql-init", "data.init.sql"),
                ),
            )
        ),
        _build_secret_rotation,
    ),
    # Delete + re-apply of the Deployment (IV:259-279); silently no-ops for any service but profile.
    entry(
        "wrong_bin_usage",
        _HOTEL,
        fixed(Pick({"faulty_service": "profile"}, (deployment("profile"),))),
        _build_wrong_bin_usage,
    ),
    # Silently no-ops for any service but geo (IV:1040-1045). Health-gated exec into the geo pod
    # (IV:1025-1027): inject early.
    entry(
        "configmap_drift",
        _HOTEL,
        fixed(
            Pick(
                {"faulty_service": "geo"},
                (
                    Footprint("ConfigMap", "geo-config"),
                    deployment("geo", "spec.template.spec.volumes"),
                    container("geo", "volumeMounts"),
                ),
                Tier.RESTARTS_PODS,
            )
        ),
        _build_configmap_drift,
    ),
    # Only frontend-proxy has shadow vars (IV:1386-1392). Delete + re-apply, and recovery re-applies
    # the full snapshot.
    entry(
        "env_variable_shadowing",
        _ASTRONOMY,
        fixed(Pick({"faulty_service": "frontend-proxy"}, (deployment("frontend-proxy"),))),
        _build_env_variable_shadowing,
    ),
    entry("file_descriptor_exhaustion", _HOTEL, _choose_fd_exhaustion, _build_fd_exhaustion),
    entry(
        "expired_tls_hotel_reservation",
        _HOTEL,
        fixed(
            Pick(
                {},
                (Footprint("Secret", "hotel-frontend-tls"), Footprint("Ingress", "frontend-ingress", "spec.tls")),
            )
        ),
        _build_expired_tls,
    ),
    entry(
        "ingress_misroute",
        _HOTEL,
        fixed(
            Pick(
                {},
                (
                    Footprint("Ingress", "hotel-reservation-ingress"),
                    Footprint("Service", "frontend-service"),
                    Footprint("Service", "recommendation-service"),
                ),
            )
        ),
        _build_ingress_misroute,
    ),
    entry(
        "dev_shm_exhaustion_hotel_reservation",
        _HOTEL,
        fixed(Pick({}, (deployment("media-processor"),))),
        _build_dev_shm,
    ),
    # Rollout-restarts frontend and recommendation and waits (raising) for their pods to run
    # (internal_traffic_policy_local.py:171-206): inject early.
    entry(
        "internal_traffic_policy_local_astronomy_shop",
        _ASTRONOMY,
        fixed(
            Pick(
                {},
                (
                    Footprint("Service", "recommendation", "spec.internalTrafficPolicy"),
                    deployment("recommendation", "spec.template.spec.nodeSelector"),
                    deployment("frontend", "spec.template.spec.nodeSelector"),
                ),
                Tier.RESTARTS_PODS,
            )
        ),
        _build_internal_traffic_policy,
    ),
    entry(
        "cronjob_sidecar_blocks_completion_hotel_reservation",
        _HOTEL,
        fixed(Pick({}, (Footprint("CronJob", "audit-log-archiver"),))),
        _build_cronjob_sidecar,
    ),
    entry("network_policy_block", _NETWORK_POLICY_APPS, _choose_network_policy, _build_network_policy),
)
