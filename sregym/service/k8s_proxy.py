"""
Kubernetes API Filtering Proxy

This proxy sits between agents and the Kubernetes API server, filtering out
chaos engineering namespaces (chaos-mesh, khaos) and load generator resources
from API responses to prevent agents from discovering that faults are being
injected via chaos tools or that traffic is synthetic.

The proxy:
1. Forwards all requests to the real Kubernetes API
2. Filters namespace listings to exclude hidden namespaces
3. Returns 403 Forbidden for direct access to hidden namespaces or hidden resources
4. Filters cluster-wide resource listings to exclude resources in hidden namespaces
5. Filters resources with hidden labels (e.g. load generators) from list responses
6. Streams follow-mode logs and watches, filtering each watch event like a list item
7. Forwards pod exec/attach/port-forward connection upgrades (SPDY or websocket)
   only when enabled (``SREGYM_AGENT_PROXY_ALLOW_EXEC``); otherwise answers them
   with a Kubernetes ``403 Forbidden`` Status so the agent learns exec is unavailable
"""

import base64
import contextlib
import http.client
import json
import logging
import os
import re
import select
import socket
import ssl
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import urllib3
import yaml
from kubernetes import config

logger = logging.getLogger("all.infra.k8s_proxy")
logger.propagate = True
logger.setLevel(logging.DEBUG)

# Namespaces to hide from agents
HIDDEN_NAMESPACES: set[str] = {"chaos-mesh", "khaos"}

# Labels to hide from agents - resources matching any of these label key/value pairs are hidden.
# Load generators produce synthetic traffic and should not be visible to agents.
HIDDEN_LABELS: dict[str, set[str]] = {
    "app": {"load-generator", "locust-fetcher"},
    "job": {"workload"},
    "opentelemetry.io/name": {"load-generator"},
}

#: Environment switch for forwarding pod exec/attach/port-forward through the agent proxy.
ALLOW_EXEC_ENV = "SREGYM_AGENT_PROXY_ALLOW_EXEC"
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"", "0", "false", "no", "off"}

#: Pod subresources that need an HTTP connection upgrade (SPDY or websocket).
UPGRADE_SUBRESOURCES = ("exec", "attach", "portforward")
_POD_UPGRADE_PATH = re.compile(
    r"^/api/v1/namespaces/(?P<namespace>[^/?]+)/pods/(?P<pod>[^/?]+)/(?P<subresource>exec|attach|portforward)/?(?:\?|$)"
)
_POD_LOG_PATH = re.compile(r"^/api/v1/namespaces/[^/?]+/pods/[^/?]+/log/?$")
_STREAM_CHUNK = 64 * 1024


def allow_exec_from_env(environ: dict[str, str] | None = None) -> bool:
    """Whether ``SREGYM_AGENT_PROXY_ALLOW_EXEC`` enables exec forwarding; off when unset."""

    raw = (environ if environ is not None else os.environ).get(ALLOW_EXEC_ENV, "").strip().lower()
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    raise ValueError(f"{ALLOW_EXEC_ENV} must be one of {sorted(_TRUE_VALUES | _FALSE_VALUES)}, got {raw!r}")


def pod_upgrade_target(path: str) -> tuple[str, str, str] | None:
    """``(namespace, pod, subresource)`` for a pod exec/attach/portforward path, else ``None``."""

    match = _POD_UPGRADE_PATH.match(path)
    if match is None:
        return None
    return match["namespace"], match["pod"], match["subresource"]


def streaming_kind(path: str) -> str | None:
    """``"watch"`` or ``"follow"`` when the upstream response is an unbounded stream."""

    parsed = urlparse(path)
    query = parse_qs(parsed.query)
    if "/watch/" in parsed.path or query.get("watch", ["false"])[-1].lower() in ("true", "1"):
        return "watch"
    if _POD_LOG_PATH.match(parsed.path) and query.get("follow", ["false"])[-1].lower() in ("true", "1"):
        return "follow"
    return None


def forbidden_status(message: str) -> bytes:
    """A Kubernetes ``Status`` body kubectl prints verbatim as ``Error from server (Forbidden)``."""

    return json.dumps(
        {
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": message,
            "reason": "Forbidden",
            "code": 403,
        }
    ).encode()


# Disable SSL warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KubernetesAPIProxy:
    """Manages the Kubernetes API filtering proxy."""

    # Paths used when running inside a Kubernetes pod
    _INCLUSTER_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    _INCLUSTER_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    def __init__(
        self,
        hidden_namespaces: set[str] | None = None,
        hidden_labels: dict[str, set[str]] | None = None,
        listen_port: int = 6443,
        allow_exec: bool | None = None,
    ):
        if allow_exec is not None and not isinstance(allow_exec, bool):
            raise TypeError(f"allow_exec must be a bool or None, got {type(allow_exec).__name__}")
        #: Forward pod exec/attach/port-forward upgrades; ``None`` reads ``SREGYM_AGENT_PROXY_ALLOW_EXEC``.
        self.allow_exec: bool = allow_exec_from_env() if allow_exec is None else allow_exec
        self.hidden_namespaces: set[str] = hidden_namespaces if hidden_namespaces is not None else HIDDEN_NAMESPACES
        self.hidden_labels: dict[str, set[str]] = hidden_labels if hidden_labels is not None else HIDDEN_LABELS
        self.listen_port = listen_port
        #: Namespaces where exec/attach/port-forward may run when ``allow_exec`` is on; the
        #: conductor sets the current problem's application namespaces. Empty denies all.
        self.exec_namespaces: set[str] = set()
        self.server: ThreadingHTTPServer | None = None
        self.server_thread: threading.Thread | None = None
        self._temp_files: list = []
        self._bearer_token: str | None = None

        if os.path.exists(self._INCLUSTER_TOKEN_PATH):
            # Running inside a Kubernetes pod — use ServiceAccount credentials
            logger.info("Detected in-cluster environment; using ServiceAccount token for upstream auth")
            with open(self._INCLUSTER_TOKEN_PATH) as f:
                self._bearer_token = f.read().strip()
            with open(self._INCLUSTER_CA_PATH) as f:
                self.ca_cert = f.read()
            self.client_cert = None
            self.client_key = None
            self.api_host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
            self.api_port = int(os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
        else:
            # Running outside the cluster — load from kubeconfig
            # Prefer the supervisor-provided base config so KUBECONFIG may later
            # point at this proxy without creating a circular dependency.
            default_kubeconfig = os.environ.get("SREGYM_BASE_KUBECONFIG") or os.environ.get(
                "KUBECONFIG", os.path.expanduser("~/.kube/config")
            )
            config.load_kube_config(config_file=default_kubeconfig)
            self.api_host, self.api_port, self.ca_cert, self.client_cert, self.client_key = self._load_cluster_config(
                kubeconfig_path=default_kubeconfig
            )

    def _load_cluster_config(self, kubeconfig_path: str | None = None):
        """Extract API server connection details from kubeconfig."""
        # Load full kubeconfig
        if kubeconfig_path is None:
            kubeconfig_path = os.path.expanduser("~/.kube/config")

        # Get the current context's cluster and user from the explicit config file
        _, active_context = config.list_kube_config_contexts(config_file=kubeconfig_path)
        cluster_name = active_context["context"]["cluster"]
        user_name = active_context["context"]["user"]
        with open(kubeconfig_path) as f:
            import yaml

            kubeconfig = yaml.safe_load(f)

        # Find cluster config
        cluster_config = None
        for cluster in kubeconfig["clusters"]:
            if cluster["name"] == cluster_name:
                cluster_config = cluster["cluster"]
                break

        # Find user config
        user_config = None
        for user in kubeconfig["users"]:
            if user["name"] == user_name:
                user_config = user["user"]
                break

        if not cluster_config:
            raise ValueError(f"Cluster {cluster_name} not found in kubeconfig")

        # Parse API server URL
        server_url = cluster_config["server"]
        parsed = urlparse(server_url)
        api_host = parsed.hostname
        api_port = parsed.port or 443

        # Get CA cert (might be inline or file path)
        ca_cert = None
        if "certificate-authority-data" in cluster_config:
            ca_cert = base64.b64decode(cluster_config["certificate-authority-data"]).decode()
        elif "certificate-authority" in cluster_config:
            with open(cluster_config["certificate-authority"]) as f:
                ca_cert = f.read()

        # Get client cert and key
        client_cert = None
        client_key = None
        if user_config:
            if "client-certificate-data" in user_config:
                client_cert = base64.b64decode(user_config["client-certificate-data"]).decode()
            elif "client-certificate" in user_config:
                with open(user_config["client-certificate"]) as f:
                    client_cert = f.read()

            if "client-key-data" in user_config:
                client_key = base64.b64decode(user_config["client-key-data"]).decode()
            elif "client-key" in user_config:
                with open(user_config["client-key"]) as f:
                    client_key = f.read()

        return api_host, api_port, ca_cert, client_cert, client_key

    def _create_temp_cert_files(self):
        """Create temporary files for certificates."""
        files = {}

        if self.ca_cert:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as ca_file:
                ca_file.write(self.ca_cert)
            files["ca"] = ca_file.name
            self._temp_files.append(ca_file.name)

        if self.client_cert:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as cert_file:
                cert_file.write(self.client_cert)
            files["cert"] = cert_file.name
            self._temp_files.append(cert_file.name)

        if self.client_key:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as key_file:
                key_file.write(self.client_key)
            files["key"] = key_file.name
            self._temp_files.append(key_file.name)

        return files

    def start(self):
        """Start the proxy server in a background thread."""
        cert_files = self._create_temp_cert_files()
        hidden_namespaces = self.hidden_namespaces
        hidden_labels = self.hidden_labels
        api_host = self.api_host
        api_port = self.api_port
        bearer_token = self._bearer_token
        proxy = self  # exec policy is read per request, so the conductor can scope it per problem

        class FilteringProxyHandler(BaseHTTPRequestHandler):
            """HTTP request handler that proxies and filters Kubernetes API responses."""

            def log_message(self, format, *args):
                logger.debug(f"Proxy: {format % args}")

            def _upstream_ssl_context(self) -> ssl.SSLContext:
                """TLS context that authenticates to the upstream Kubernetes API."""
                context = ssl.create_default_context()
                if cert_files.get("ca"):
                    context.load_verify_locations(cert_files["ca"])
                else:
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE

                if cert_files.get("cert") and cert_files.get("key"):
                    context.load_cert_chain(cert_files["cert"], cert_files["key"])
                return context

            def _get_upstream_connection(self):
                """Create HTTPS connection to upstream Kubernetes API."""
                return http.client.HTTPSConnection(api_host, api_port, context=self._upstream_ssl_context())

            def _send_forbidden(self, message: str) -> None:
                """Answer with a Kubernetes 403 Status and close the connection."""
                body = forbidden_status(message)
                self.close_connection = True
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def _pod_is_hidden(self, namespace: str, pod: str) -> bool:
                """Whether the target pod carries a hidden label (e.g. a load generator)."""
                conn = self._get_upstream_connection()
                headers = {"Accept": "application/json"}
                if bearer_token:
                    headers["Authorization"] = f"Bearer {bearer_token}"
                try:
                    conn.request("GET", f"/api/v1/namespaces/{namespace}/pods/{pod}", headers=headers)
                    response = conn.getresponse()
                    body = response.read()
                    if response.status != 200:
                        return False  # upstream reports the missing pod itself
                    return self._has_hidden_label(json.loads(body).get("metadata", {}))
                finally:
                    conn.close()

            def _proxy_upgrade(self, method: str, path: str) -> None:
                """Forward a pod exec/attach/portforward upgrade, or refuse it with a 403 Status."""
                target = pod_upgrade_target(path)
                if target is None:
                    self._send_forbidden(
                        "Forbidden: the SREGym agent API proxy forwards connection upgrades only for "
                        "pod exec, attach and port-forward"
                    )
                    return
                namespace, pod, subresource = target
                command = {"exec": "kubectl exec", "attach": "kubectl attach", "portforward": "kubectl port-forward"}[
                    subresource
                ]
                if not proxy.allow_exec:
                    self._send_forbidden(
                        f'pods "{pod}" is forbidden: {command} is disabled in this environment; '
                        "the agent API proxy does not forward pod exec, attach or port-forward. "
                        "Use get, describe, logs and events to inspect workloads instead."
                    )
                    return
                if namespace not in proxy.exec_namespaces:
                    allowed = ", ".join(sorted(proxy.exec_namespaces)) or "none"
                    self._send_forbidden(
                        f'pods "{pod}" is forbidden: {command} is allowed only in the application '
                        f"namespace(s) ({allowed}), not in {namespace!r}"
                    )
                    return
                try:
                    hidden = self._pod_is_hidden(namespace, pod)
                except Exception as e:
                    logger.error(f"Proxy error checking pod {namespace}/{pod}: {e}")
                    self.send_error(502, f"Bad Gateway: {str(e)}")
                    return
                if hidden:
                    self._send_forbidden("Forbidden: Access to this resource is not allowed")
                    return
                self._tunnel(method, path)

            def _tunnel(self, method: str, path: str) -> None:
                """Replay the upgrade request upstream, then splice both sockets byte for byte.

                The upstream answer (``101 Switching Protocols`` or an error) reaches the
                client unmodified, so SPDY and websocket framing pass through intact.
                """
                self.close_connection = True
                try:
                    raw = socket.create_connection((api_host, api_port), timeout=30)
                    upstream = self._upstream_ssl_context().wrap_socket(raw, server_hostname=api_host)
                    upstream.settimeout(None)
                except Exception as e:
                    logger.error(f"Proxy error opening upgrade tunnel: {e}")
                    self.send_error(502, f"Bad Gateway: {str(e)}")
                    return
                try:
                    lines = [f"{method} {path} HTTP/1.1", f"Host: {api_host}:{api_port}"]
                    for header, value in self.headers.items():  # keeps repeated protocol headers
                        if header.lower() == "host" or (bearer_token and header.lower() == "authorization"):
                            continue
                        lines.append(f"{header}: {value}")
                    if bearer_token:
                        lines.append(f"Authorization: Bearer {bearer_token}")
                    upstream.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
                    # Bytes the client sent after its headers may sit in rfile's buffer.
                    self.connection.setblocking(False)
                    try:
                        pending = self.rfile.read1(_STREAM_CHUNK)
                    except BlockingIOError:
                        pending = b""
                    finally:
                        self.connection.setblocking(True)
                    if pending:
                        upstream.sendall(pending)
                    self._splice(self.connection, upstream)
                except OSError:
                    pass  # either side hung up
                finally:
                    upstream.close()

            @staticmethod
            def _splice(client: socket.socket, upstream: ssl.SSLSocket) -> None:
                """Copy bytes both ways until either side closes (one thread: an SSL socket is not thread-safe)."""
                peers = {client: upstream, upstream: client}
                while True:
                    readable, _, _ = select.select(list(peers), [], [])
                    for sock in readable:
                        data = sock.recv(_STREAM_CHUNK)
                        if not data:
                            return
                        peers[sock].sendall(data)
                        # Decrypted TLS bytes already buffered are invisible to select.
                        while isinstance(sock, ssl.SSLSocket) and sock.pending():
                            peers[sock].sendall(sock.recv(sock.pending()))

            def _filter_watch_event(self, line: bytes, namespaces_watch: bool) -> bytes | None:
                """Filter one watch event like a list item; ``None`` drops it."""
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    return line
                obj = event.get("object") if isinstance(event, dict) else None
                if not isinstance(obj, dict):
                    return line
                if "rows" in obj:  # Table-format watch (kubectl get -w)
                    obj = self._filter_namespace_list(obj) if namespaces_watch else self._filter_resource_list(obj)
                    if not obj["rows"]:
                        return None
                    return (json.dumps(event) + "\n").encode()
                metadata = obj.get("metadata") or {}
                if metadata.get("namespace") in hidden_namespaces or self._has_hidden_label(metadata):
                    return None
                if namespaces_watch and metadata.get("name") in hidden_namespaces:
                    return None
                return line

            def _relay_stream(self, response, kind: str, path: str) -> None:
                """Relay an unbounded watch or follow-mode log as it arrives instead of buffering it."""
                self.close_connection = True  # HTTP/1.0: the body ends when the connection closes
                self.send_response(response.status)
                for header, value in response.getheaders():
                    if header.lower() not in ("transfer-encoding", "content-length", "content-encoding", "connection"):
                        self.send_header(header, value)
                self.send_header("Connection", "close")
                self.end_headers()
                parsed_path = urlparse(path).path.rstrip("/")
                namespaces_watch = parsed_path in ("/api/v1/namespaces", "/api/v1/watch/namespaces")
                try:
                    if kind == "follow":
                        while chunk := response.read1(_STREAM_CHUNK):
                            self.wfile.write(chunk)
                            self.wfile.flush()
                        return
                    while line := response.readline():
                        filtered = self._filter_watch_event(line, namespaces_watch)
                        if filtered is not None:
                            self.wfile.write(filtered)
                            self.wfile.flush()
                except (OSError, http.client.HTTPException):
                    pass  # the agent stopped watching, or the upstream stream ended early

            def _is_hidden_namespace_request(self, path: str) -> bool:
                """Check if request is for a hidden namespace."""
                # Direct namespace access: /api/v1/namespaces/{namespace}
                # Resources in namespace: /api/v1/namespaces/{namespace}/...
                # or /apis/{group}/{version}/namespaces/{namespace}/...
                parts = path.split("/")
                for i, part in enumerate(parts):
                    if part == "namespaces" and i + 1 < len(parts):
                        ns = parts[i + 1].split("?")[0]  # Remove query params
                        if ns in hidden_namespaces:
                            return True
                return False

            def _filter_namespace_list(self, data: dict) -> dict:
                """Filter hidden namespaces from namespace list response."""
                # Handle standard List format
                if "items" in data:
                    data["items"] = [
                        item for item in data["items"] if item.get("metadata", {}).get("name") not in hidden_namespaces
                    ]
                # Handle Table format (kubectl's default)
                if "rows" in data:
                    data["rows"] = [
                        row
                        for row in data["rows"]
                        if row.get("object", {}).get("metadata", {}).get("name") not in hidden_namespaces
                    ]
                return data

            def _has_hidden_label(self, metadata: dict) -> bool:
                """Check if a resource's metadata contains any hidden labels."""
                labels = metadata.get("labels") or {}
                return any(labels.get(key) in values for key, values in hidden_labels.items())

            def _filter_resource_list(self, data: dict) -> dict:
                """Filter resources in hidden namespaces or with hidden labels from list responses."""
                # Handle standard List format
                if "items" in data:
                    data["items"] = [
                        item
                        for item in data["items"]
                        if item.get("metadata", {}).get("namespace") not in hidden_namespaces
                        and not self._has_hidden_label(item.get("metadata", {}))
                    ]
                # Handle Table format (kubectl's default)
                if "rows" in data:
                    data["rows"] = [
                        row
                        for row in data["rows"]
                        if row.get("object", {}).get("metadata", {}).get("namespace") not in hidden_namespaces
                        and not self._has_hidden_label(row.get("object", {}).get("metadata", {}))
                    ]
                return data

            def _should_filter_response(self, path: str) -> str | None:
                """
                Determine if response should be filtered and return filter type.
                Returns: 'namespaces', 'resources', or None
                """
                # Namespace list: /api/v1/namespaces
                if path.rstrip("/") == "/api/v1/namespaces" or path.startswith("/api/v1/namespaces?"):
                    return "namespaces"

                # Cluster-wide resource listings (not namespaced)
                # e.g., /api/v1/pods, /api/v1/events, /apis/apps/v1/deployments
                if "/namespaces/" not in path:
                    # Check if this is a list of namespaced resources
                    resource_patterns = [
                        "/api/v1/pods",
                        "/api/v1/services",
                        "/api/v1/events",
                        "/api/v1/configmaps",
                        "/api/v1/secrets",
                        "/api/v1/endpoints",
                        "/api/v1/persistentvolumeclaims",
                        "/apis/apps/v1/deployments",
                        "/apis/apps/v1/replicasets",
                        "/apis/apps/v1/statefulsets",
                        "/apis/apps/v1/daemonsets",
                        "/apis/batch/v1/jobs",
                        "/apis/batch/v1/cronjobs",
                    ]
                    for pattern in resource_patterns:
                        if path.startswith(pattern):
                            return "resources"

                return None

            def _proxy_request(self, method: str):
                """Proxy request to upstream API and filter response."""
                path = self.path

                # Block direct access to hidden namespaces
                if self._is_hidden_namespace_request(path):
                    self.send_error(403, "Forbidden: Access to this namespace is not allowed")
                    return

                if pod_upgrade_target(path) is not None or self.headers.get("Upgrade"):
                    self._proxy_upgrade(method, path)
                    return
                stream = streaming_kind(path) if method == "GET" else None

                # Read request body if present
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length) if content_length > 0 else None

                # Forward request to upstream
                try:
                    conn = self._get_upstream_connection()
                    # Forward headers (except Host and Accept-Encoding to avoid gzip)
                    headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "accept-encoding")}
                    # In-cluster mode: authenticate to the API server with the ServiceAccount bearer token
                    if bearer_token:
                        headers["Authorization"] = f"Bearer {bearer_token}"
                    conn.request(method, path, body=body, headers=headers)
                    response = conn.getresponse()

                    if stream is not None and response.status == 200:
                        self._relay_stream(response, stream, path)
                        conn.close()
                        return

                    # Read response
                    response_body = response.read()
                    content_type = response.getheader("Content-Type", "")
                    content_encoding = response.getheader("Content-Encoding", "")

                    # Decompress if gzip-encoded
                    if content_encoding == "gzip":
                        import gzip

                        response_body = gzip.decompress(response_body)

                    # Filter JSON responses if needed
                    filter_type = self._should_filter_response(path)
                    if response.status == 200 and "application/json" in content_type:
                        try:
                            data = json.loads(response_body)
                            if filter_type == "namespaces":
                                data = self._filter_namespace_list(data)
                                response_body = json.dumps(data).encode()
                            elif filter_type == "resources":
                                data = self._filter_resource_list(data)
                                response_body = json.dumps(data).encode()
                            elif filter_type is None and self._has_hidden_label(data.get("metadata", {})):
                                # Block direct access to individual hidden resources
                                self.send_error(403, "Forbidden: Access to this resource is not allowed")
                                conn.close()
                                return
                        except json.JSONDecodeError:
                            pass  # Not valid JSON, pass through as-is

                    # Send response to client
                    self.send_response(response.status)
                    for header, value in response.getheaders():
                        # Skip headers we're modifying
                        if header.lower() not in ("transfer-encoding", "content-length", "content-encoding"):
                            self.send_header(header, value)
                    self.send_header("Content-Length", str(len(response_body)))
                    self.end_headers()
                    self.wfile.write(response_body)

                    conn.close()

                except BrokenPipeError:
                    # Client closed while agent still has in-flight request open. Ignore
                    pass
                except Exception as e:
                    logger.error(f"Proxy error: {e}")
                    self.send_error(502, f"Bad Gateway: {str(e)}")

            def do_GET(self):
                self._proxy_request("GET")

            def do_POST(self):
                self._proxy_request("POST")

            def do_PUT(self):
                self._proxy_request("PUT")

            def do_PATCH(self):
                self._proxy_request("PATCH")

            def do_DELETE(self):
                self._proxy_request("DELETE")

            def do_OPTIONS(self):
                self._proxy_request("OPTIONS")

            def do_HEAD(self):
                self._proxy_request("HEAD")

        # Create and start server
        self.server = ThreadingHTTPServer(("127.0.0.1", self.listen_port), FilteringProxyHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        logger.info(f"Kubernetes API filtering proxy started on port {self.listen_port}")
        logger.info(f"Hidden namespaces: {self.hidden_namespaces}")
        logger.info(f"Hidden labels: {self.hidden_labels}")

    def stop(self):
        """Stop the proxy server."""
        if self.server:
            self.server.shutdown()
            self.server = None
            self.server_thread = None
            logger.info("Kubernetes API filtering proxy stopped")

        # Cleanup temp files
        for temp_file in self._temp_files:
            with contextlib.suppress(OSError):
                os.unlink(temp_file)
        self._temp_files = []

    def generate_agent_kubeconfig(self, output_path: str | None = None) -> str:
        """
        Generate a kubeconfig file for agents that points to this proxy.

        Args:
            output_path: Path to write kubeconfig. If None, writes to temp file.

        Returns:
            Path to the generated kubeconfig file.
        """
        import yaml

        kubeconfig = {
            "apiVersion": "v1",
            "kind": "Config",
            "current-context": "sregym-agent",
            "clusters": [
                {
                    "name": "sregym-proxy",
                    "cluster": {
                        # Use HTTP since proxy runs locally without TLS
                        "server": f"http://127.0.0.1:{self.listen_port}",
                        # Skip TLS verification for local proxy
                        "insecure-skip-tls-verify": True,
                    },
                }
            ],
            "contexts": [
                {
                    "name": "sregym-agent",
                    "context": {
                        "cluster": "sregym-proxy",
                        "user": "sregym-agent",
                    },
                }
            ],
            "users": [
                {
                    "name": "sregym-agent",
                    # No credentials needed - proxy handles auth to real API
                    "user": {},
                }
            ],
        }

        if output_path is None:
            # One file per proxy port: concurrent conductors must not overwrite each other's agent
            # kubeconfig, or an agent is silently pointed at another cluster's proxy.
            output_path = os.path.join(tempfile.gettempdir(), f"sregym-agent-kubeconfig-p{self.listen_port}")

        with open(output_path, "w") as f:
            yaml.dump(kubeconfig, f)

        logger.info(f"Generated agent kubeconfig at {output_path}")
        return output_path

    def get_proxy_url(self) -> str:
        """Get the URL of the proxy server."""
        return f"http://127.0.0.1:{self.listen_port}"


# Module-level singleton for easy access
_proxy_instance: KubernetesAPIProxy | None = None


def get_proxy() -> KubernetesAPIProxy:
    """Get or create the singleton proxy instance."""
    global _proxy_instance
    if _proxy_instance is None:
        _proxy_instance = KubernetesAPIProxy()
    return _proxy_instance


class AgentKubeconfigMismatch(RuntimeError):
    """The kubeconfig handed to agents does not reach this experiment's own cluster."""


def verify_agent_kubeconfig(
    path: str,
    *,
    listen_port: int,
    cluster_name: str | None,
    runner: Any = subprocess.run,
) -> None:
    """Fail loudly unless the agent kubeconfig reaches this conductor's proxy and cluster.

    The agent kubeconfig (mounted into agent containers, and ``KUBECONFIG`` for
    host-side agents) must name exactly this proxy's port, and, for a kind
    cluster, every node seen through it must belong to ``cluster_name``.
    """

    with open(path, encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    servers = [str((item.get("cluster") or {}).get("server")) for item in document.get("clusters") or []]
    expected = f"http://127.0.0.1:{listen_port}"
    if servers != [expected]:
        raise AgentKubeconfigMismatch(f"agent kubeconfig {path} targets {servers}, expected this proxy {expected}")
    if not cluster_name:
        return
    completed = runner(
        ["kubectl", "--kubeconfig", path, "get", "nodes", "-o", "name", "--request-timeout=20s"],
        capture_output=True,
        text=True,
        check=False,
    )
    nodes = [line.strip().removeprefix("node/") for line in completed.stdout.splitlines() if line.strip()]
    foreign = [node for node in nodes if not node.startswith(f"{cluster_name}-")]
    if completed.returncode != 0 or not nodes or foreign:
        details = completed.stderr.strip() if completed.returncode != 0 else f"nodes {foreign or nodes}"
        raise AgentKubeconfigMismatch(f"agent kubeconfig {path} does not reach cluster {cluster_name} only: {details}")
    logger.info("Agent kubeconfig %s verified: port %s reaches only %s", path, listen_port, cluster_name)


def agent_proxy_port() -> int:
    """Filtering-proxy port for this worker: ``16443 + SREGYM_WORKER_ID``.

    The proxy binds a host port, so concurrent conductors need distinct ones.
    """

    raw = os.environ.get("SREGYM_WORKER_ID", "").strip()
    return 16443 + (int(raw) if raw else 0)


def start_proxy(
    hidden_namespaces: set[str] | None = None,
    hidden_labels: dict[str, set[str]] | None = None,
    port: int = 16443,
) -> KubernetesAPIProxy:
    """Start the Kubernetes API filtering proxy."""
    global _proxy_instance
    if _proxy_instance is not None:
        _proxy_instance.stop()
    _proxy_instance = KubernetesAPIProxy(
        hidden_namespaces=hidden_namespaces, hidden_labels=hidden_labels, listen_port=port
    )
    _proxy_instance.start()
    return _proxy_instance


def stop_proxy():
    """Stop the Kubernetes API filtering proxy."""
    global _proxy_instance
    if _proxy_instance is not None:
        _proxy_instance.stop()
        _proxy_instance = None
