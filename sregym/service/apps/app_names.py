from __future__ import annotations

from enum import StrEnum
from typing import Iterable


class AppName(StrEnum):
    ASTRONOMY_SHOP = "Astronomy Shop"
    HOTEL_RESERVATION = "Hotel Reservation"
    SOCIAL_NETWORK = "Social Network"
    FLEET_CAST = "Fleet Cast"
    BLUEPRINT_HOTEL_RESERVATION = "Blueprint Hotel Reservation"
    TRAIN_TICKET = "Train Ticket"


CLI_APP_NAME_ALIASES: dict[str, AppName] = {
    "astronomy_shop": AppName.ASTRONOMY_SHOP,
    "hotel_reservation": AppName.HOTEL_RESERVATION,
    "social_network": AppName.SOCIAL_NETWORK,
    "train_ticket": AppName.TRAIN_TICKET,
    "fleet_cast": AppName.FLEET_CAST,
    "blueprint_hotel_reservation": AppName.BLUEPRINT_HOTEL_RESERVATION,
}


def canonical_app_name(app_name: str | AppName) -> str:
    if isinstance(app_name, AppName):
        return app_name.value

    normalized = app_name.strip()
    alias_key = normalized.lower().replace("-", "_").replace(" ", "_")
    if alias_key in CLI_APP_NAME_ALIASES:
        return CLI_APP_NAME_ALIASES[alias_key].value

    for enum_name in AppName:
        if normalized.lower() == enum_name.value.lower():
            return enum_name.value

    return app_name


def canonical_app_names(app_names: Iterable[str | AppName]) -> frozenset[str]:
    return frozenset(canonical_app_name(app_name) for app_name in app_names)


def resolve_cli_app_name(app_name: str | AppName) -> str:
    normalized = app_name.value if isinstance(app_name, AppName) else app_name.strip()
    alias_key = normalized.lower().replace("-", "_").replace(" ", "_")
    if alias_key in CLI_APP_NAME_ALIASES:
        return CLI_APP_NAME_ALIASES[alias_key].value

    for display_name in CLI_APP_NAME_ALIASES.values():
        if normalized.lower() == display_name.lower():
            return display_name.value

    raise ValueError(
        f"Unknown app '{app_name}'. Valid app names: {sorted(CLI_APP_NAME_ALIASES)}"
    )
