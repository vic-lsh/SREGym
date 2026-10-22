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
"""Catalog family: feature-flag faults (astronomy_shop OTel flagd, TrainTicket F17/F22) and Kafka.

Covers the READY and FIXABLE rows of audit part 4. Each flag fault's footprint is its own flag
object inside the flagd ConfigMap's JSON/YAML data key, so different flags compose and the same
flag conflicts. The base registry id and the VariantSpec of AdServiceManualGc and CartServiceFailure
toggle the same flag, so each appears once.

Tiers:
- Every OTel injector restarts flagd and the flag's consumer Deployments on inject and recover
  (inject_otel.py:13-59). Those restarts sit outside the flag footprint and wipe runtime-only state
  of earlier faults on the consumer (e.g. SecretRotation's pod-UID staleness on product-catalog, the
  edge driver in load-generator), so OTel faults are RESTARTS_PODS: injected first.
  Restarts are not footprints: two flags sharing a consumer (adFailure / adManualGc on ad) do not
  overwrite each other's state, and declaring the restart would make them conflict.
- TrainTicket injectors restart only the train-ticket flagd, which holds no other fault: ORDINARY.
- KafkaPoisonPill is health-gated on the broker, DNS and egress (inject_kafka.py:441,453): first.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from typing import Any

from sregym.conductor.problems.composition.catalog._kit import Pick, container, deployment, entry, fixed
from sregym.conductor.problems.composition.spec import CatalogEntry, Footprint, Tier

_ASTRONOMY = ("astronomy_shop",)
_TRAIN_TICKET = ("train_ticket",)

_OTEL_CONFIGMAP = "flagd-config"
_OTEL_DATA_KEY = "demo.flagd.json"
_TT_DATA_KEY = "flags.yaml"

# Shared by the two faults whose liveness depends on Kafka broker state: KafkaQueueProblems restarts
# the broker (on inject and recover), which can drop the poison topic and consumer offsets.
_KAFKA_BROKER_TAG = "state:astronomy_shop/kafka-broker"  # `state:` = in-namespace (composition/recovery.py)


def flag_footprint(data_key: str, flag: str) -> Footprint:
    """The flag object inside the flagd ConfigMap data key, e.g. `data.demo.flagd.json.flags.adFailure`."""
    return Footprint("ConfigMap", _OTEL_CONFIGMAP, f"data.{data_key}.flags.{flag}")


def _otel(
    key: str,
    flag: str,
    build: Callable[[str, dict[str, Any]], Any],
    extra: tuple[Footprint, ...] = (),
    tags: frozenset[str] = frozenset(),
    params: dict[str, Any] | None = None,
) -> CatalogEntry:
    pick = Pick(params or {}, (flag_footprint(_OTEL_DATA_KEY, flag), *extra), Tier.RESTARTS_PODS, tags)
    return entry(key, _ASTRONOMY, fixed(pick), build)


def _build_ad_failure(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.ad_service_failure import AdServiceFailure

    return AdServiceFailure()


def _build_ad_high_cpu(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.ad_service_high_cpu import AdServiceHighCpu

    return AdServiceHighCpu()


def _build_image_slow_load(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.image_slow_load import ImageSlowLoad

    return ImageSlowLoad()


def _build_ad_manual_gc(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.ad_service_manual_gc import AdServiceManualGc

    return AdServiceManualGc(**params)


def _build_cart_failure(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.cart_service_failure import CartServiceFailure

    return CartServiceFailure(**params)


def _build_failed_readiness_probe(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.failed_readiness_probe import FailedReadinessProbe

    return FailedReadinessProbe()


def _build_payment_failure(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.payment_service_failure import PaymentServiceFailure

    return PaymentServiceFailure()


def _build_payment_unreachable(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.payment_service_unreachable import PaymentServiceUnreachable

    return PaymentServiceUnreachable()


def _build_product_catalog_failure(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.product_catalog_failure import ProductCatalogServiceFailure

    return ProductCatalogServiceFailure()


def _build_kafka_queue_problems(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.kafka_queue_problems import KafkaQueueProblems

    return KafkaQueueProblems()


def _build_loadgenerator_flood(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.loadgenerator_flood_homepage import LoadGeneratorFloodHomepage

    return LoadGeneratorFloodHomepage()


def _build_kafka_poison_pill(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.kafka_poison_pill_hol_block import KafkaPoisonPillHOLBlock

    return KafkaPoisonPillHOLBlock()


# --- TrainTicket F17 / F22 ---------------------------------------------------------------------------

_DECOY_FLAG_COUNTS = ("none", "low", "high")


def _tt_choose(flag: str) -> Callable[[str, random.Random], Pick]:
    # Decoy flags are not footprints: they are idempotent "on" writes to dud flags (never 17 or 22,
    # inject_tt.py:23) and recovery turns all of them off, which no fault depends on.
    def choose(app: str, rng: random.Random) -> Pick:
        decoys = rng.choice(_DECOY_FLAG_COUNTS)
        return Pick({"decoy_flag_count": decoys}, (flag_footprint(_TT_DATA_KEY, flag),))

    return choose


def _build_tt_f17(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.trainticket_f17 import TrainTicketF17

    return TrainTicketF17(**params)


def _build_tt_f22(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.train_ticket_f22 import TrainTicketF22

    return TrainTicketF22(**params)


ENTRIES: tuple[CatalogEntry, ...] = (
    _otel("astronomy_shop_ad_service_failure", "adFailure", _build_ad_failure),
    # Also patches the ad cpu limit to 100m (ad_service_high_cpu.py:40-46).
    _otel(
        "astronomy_shop_ad_service_high_cpu",
        "adHighCpu",
        _build_ad_high_cpu,
        extra=(container("ad", "resources.limits.cpu"),),
    ),
    _otel("astronomy_shop_ad_service_image_slow_load", "imageSlowLoad", _build_image_slow_load),
    _otel(
        "ad_service_manual_gc",
        "adManualGc",
        _build_ad_manual_gc,
        params={"faulty_service": "ad", "feature_flag": "adManualGc"},
    ),
    _otel(
        "cart_service_failure",
        "cartFailure",
        _build_cart_failure,
        params={"faulty_service": "cart", "feature_flag": "cartFailure"},
    ),
    # Also adds a gRPC readinessProbe to cart (failed_readiness_probe.py:41-47).
    _otel(
        "astronomy_shop_failed_readiness_probe",
        "failedReadinessProbe",
        _build_failed_readiness_probe,
        extra=(container("cart", "readinessProbe"),),
    ),
    _otel("astronomy_shop_payment_service_failure", "paymentFailure", _build_payment_failure),
    _otel("astronomy_shop_payment_service_unreachable", "paymentUnreachable", _build_payment_unreachable),
    _otel(
        "astronomy_shop_product_catalog_service_failure",
        "productCatalogFailure",
        _build_product_catalog_failure,
    ),
    _otel(
        "kafka_queue_problems",
        "kafkaQueueProblems",
        _build_kafka_queue_problems,
        tags=frozenset({_KAFKA_BROKER_TAG}),
    ),
    _otel("loadgenerator_flood_homepage", "loadGeneratorFloodHomepage", _build_loadgenerator_flood),
    # New objects only, plus the broker it depends on (whole kafka Deployment: any fault on kafka
    # breaks its seed / head-of-line wait, inject_kafka.py:272-299).
    entry(
        "kafka_poison_pill_hol_block",
        _ASTRONOMY,
        fixed(
            Pick(
                {},
                (
                    Footprint("ConfigMap", "orders-pipeline-scripts"),
                    deployment("order-stream"),
                    deployment("orders-archiver"),
                    deployment("orders-validator"),
                    deployment("kafka"),
                ),
                Tier.RESTARTS_PODS,
                frozenset({_KAFKA_BROKER_TAG}),
            )
        ),
        _build_kafka_poison_pill,
    ),
    entry("trainticket_f17", _TRAIN_TICKET, _tt_choose("tt-feat-17"), _build_tt_f17),
    entry("train_ticket_f22", _TRAIN_TICKET, _tt_choose("tt-feat-22"), _build_tt_f22),
)
