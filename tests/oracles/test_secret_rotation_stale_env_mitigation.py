"""SecretRotationStaleEnvMitigation: product-catalog and PostgreSQL agree on the rotated credential."""

import json
import re
import shlex
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.secret_rotation_stale_env_mitigation import SecretRotationStaleEnvMitigation

OLD_PW, NEW_PW = "otelp", "otelp_7k9m2q4x"
OLD = f"postgres://otelu:{OLD_PW}@postgresql/otel?sslmode=disable"
NEW = f"postgres://otelu:{NEW_PW}@postgresql/otel?sslmode=disable"
DEPLOYMENT = {
    "spec": {
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "product-catalog",
                        "env": [
                            {
                                "name": "DB_CONNECTION_STRING",
                                "valueFrom": {
                                    "secretKeyRef": {"name": "product-catalog-db-conn", "key": "DB_CONNECTION_STRING"}
                                },
                            }
                        ],
                    }
                ]
            }
        }
    }
}


class _Problem:
    """The problem's constants and helpers the oracle reads; PostgreSQL accepts `accepted` passwords."""

    namespace = "astronomy-shop"
    faulty_service = "product-catalog"
    backend_service = "postgresql"
    secret_name = "product-catalog-db-conn"
    secret_key = "DB_CONNECTION_STRING"
    db_user, db_name = "otelu", "otel"
    old_conn, new_conn, old_password, new_password = OLD, NEW, OLD_PW, NEW_PW
    _POSTGRES_PASSWORD_CHECK_ATTEMPTS = 1
    _POSTGRES_PASSWORD_CHECK_INTERVAL_SECONDS = 0

    def __init__(self, *, env, secret=NEW, accepted=(NEW_PW,), init_password=NEW_PW, ready=True):
        self.env, self.secret, self.accepted, self.init_password, self.ready = (
            env,
            secret,
            accepted,
            init_password,
            ready,
        )
        self.kubectl = SimpleNamespace(exec_command=self._exec)

    def _exec(self, command):
        if command.startswith("kubectl get deployment"):
            return json.dumps(DEPLOYMENT)
        m = re.search(r"PGPASSWORD=(\S+) psql", shlex.split(command)[-1])
        assert m, command
        return "1" if shlex.split(m.group(1))[0] in self.accepted else "0"

    def _product_catalog_pods_ready(self):
        return self.ready

    def _get_product_catalog_env(self):
        return self.env

    def _get_secret_conn_string(self):
        return self.secret

    def _postgresql_init_uses_password(self, password):
        return password == self.init_password


def _evaluate(**kwargs):
    return SecretRotationStaleEnvMitigation(_Problem(**kwargs)).evaluate()


def test_restarted_product_catalog_on_the_rotated_credential_passes():
    result = _evaluate(env=NEW)
    assert result["success"] is True, result["reason"]
    assert result["deployment_references_secret"] is True


def test_stale_env_with_the_rotated_secret_fails():
    result = _evaluate(env=OLD, accepted=(OLD_PW, NEW_PW))
    assert result["success"] is False
    assert "stale old env" in result["reason"]


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"env": NEW, "ready": False}, "not Running/Ready"),
        ({"env": OLD, "secret": OLD, "accepted": (OLD_PW,)}, "expected rotated password"),
        ({"env": NEW, "init_password": OLD_PW}, "postgresql-init"),
        ({"env": "postgres://otelu:other@postgresql/otel"}, "unexpected product-catalog"),
        ({"env": NEW, "secret": OLD}, "Secret and running env disagree"),
    ],
)
def test_inconsistent_credentials_fail(kwargs, reason):
    result = _evaluate(**kwargs)
    assert result["success"] is False
    assert reason in result["reason"]
