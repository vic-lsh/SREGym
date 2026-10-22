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
"""The conductor API tells the agent the app, never the problem, and lists no routes."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from sregym.conductor import conductor_api


@pytest.fixture
def client():
    app = SimpleNamespace(
        app_name="Hotel Reservation", namespace="hotel-reservation", namespaces=None, description="desc"
    )
    problem = type("MissingServiceHotelReservation", (), {})()
    conductor = SimpleNamespace(app=app, problem=problem, problem_id="mf__hotel_reservation__n8__s161__v_")
    conductor_api.set_conductor(conductor)
    yield TestClient(conductor_api.app)
    conductor_api.set_conductor(None)


def test_get_app_names_the_app_only(client) -> None:
    body = client.get("/get_app").json()
    assert body == {
        "app_name": "Hotel Reservation",
        "namespace": "hotel-reservation",
        "namespaces": ["hotel-reservation"],
        "descriptions": "desc",
    }
    text = client.get("/get_app").text
    assert "mf__" not in text and "MissingService" not in text


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_no_route_listing(client, path: str) -> None:
    assert client.get(path).status_code == 404
