"""Bind startup-loaded webhook certificates to pod rollout and live readiness."""

import hashlib
import ssl
import textwrap
from dataclasses import dataclass, field

from kubernetes import client

# The fault may intentionally use the wrong CA. Pin the actual peer certificate
# instead of checking trust, so a reachable backend with an old cert is not ready.
_TLS_PROBE = textwrap.dedent("""\
    import hashlib
    import socket
    import ssl
    import sys

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=2) as connection:
        with context.wrap_socket(connection) as peer:
            actual = hashlib.sha256(peer.getpeercert(binary_form=True)).hexdigest()
    if actual != sys.argv[2]:
        sys.exit("webhook TLS certificate differs from the intended certificate")
""")


@dataclass(frozen=True)
class WebhookTLS:
    """One certificate identity drives both pod replacement and TLS readiness."""

    certificate_pem: str = field(repr=False)
    port: int
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.certificate_pem, str):
            raise TypeError("certificate_pem must be a PEM string")
        if type(self.port) is not int:
            raise TypeError("port must be an integer")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        der = ssl.PEM_cert_to_DER_cert(self.certificate_pem)
        object.__setattr__(self, "fingerprint", hashlib.sha256(der).hexdigest())

    @property
    def annotations(self) -> dict[str, str]:
        return {"sregym.io/serving-certificate-sha256": self.fingerprint}

    @property
    def readiness_probe(self) -> client.V1Probe:
        return client.V1Probe(
            _exec=client.V1ExecAction(command=["python", "-c", _TLS_PROBE, str(self.port), self.fingerprint]),
            timeout_seconds=5,
            period_seconds=3,
        )
