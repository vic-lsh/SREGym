"""Utilities for problem variant generation.

Provides static mappings of known services per application and helper
functions used by variant specs and problem classes.
"""

from sregym.service.apps.astronomy_shop import AstronomyShop
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.apps.social_network import SocialNetwork
from sregym.service.apps.train_ticket import TrainTicket

APP_CLASSES = {
    "social_network": SocialNetwork,
    "hotel_reservation": HotelReservation,
    "astronomy_shop": AstronomyShop,
    "train_ticket": TrainTicket,
}

# Static mapping of known deployment names per application.
# Used by variant constraints to filter valid (app, service) combinations.
# These correspond to Kubernetes Deployment names in each app's namespace.
SERVICES_BY_APP = {
    "social_network": [
        "compose-post-service",
        "home-timeline-service",
        "media-frontend",
        "media-memcached",
        "media-mongodb",
        "media-service",
        "nginx-thrift",
        "post-storage-memcached",
        "post-storage-mongodb",
        "post-storage-service",
        "social-graph-mongodb",
        "social-graph-service",
        "text-service",
        "unique-id-service",
        "url-shorten-memcached",
        "url-shorten-mongodb",
        "url-shorten-service",
        "user-memcached",
        "user-mention-service",
        "user-mongodb",
        "user-service",
        "user-timeline-mongodb",
        "user-timeline-service",
        "write-home-timeline-service",
        "jaeger",
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
        "mongodb-geo",
        "mongodb-profile",
        "mongodb-rate",
        "mongodb-recommendation",
        "mongodb-reservation",
        "mongodb-user",
        "memcached-profile",
        "memcached-rate",
        "memcached-reservation",
    ],
    "astronomy_shop": [
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "frontend",
        "frontend-proxy",
        "load-generator",
        "payment",
        "product-catalog",
        "quote",
        "recommendation",
        "shipping",
        "image-provider",
        "flagd",
        "kafka",
        "valkey-cart",
    ],
}


def create_app(app_name: str):
    """Instantiate an application by its string name.

    Args:
        app_name: One of the keys in APP_CLASSES.

    Returns:
        An Application instance.

    Raises:
        ValueError: If app_name is not recognized.
    """
    cls = APP_CLASSES.get(app_name)
    if cls is None:
        raise ValueError(f"Unknown app: {app_name}. Valid apps: {list(APP_CLASSES.keys())}")
    return cls()


def is_valid_service(app_name: str, service: str) -> bool:
    """Check if a service is a known deployment in the given app."""
    services = SERVICES_BY_APP.get(app_name, [])
    return service in services
