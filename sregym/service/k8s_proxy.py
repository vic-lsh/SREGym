"""
Kubernetes API Filtering Proxy

This proxy sits between agents and the Kubernetes API server, filtering out
chaos engineering namespaces (chaos-mesh, khaos) from API responses to prevent
agents from discovering that faults are being injected via chaos tools.

The proxy:
1. Forwards all requests to the real Kubernetes API
2. Filters namespace listings to exclude hidden namespaces
3. Returns 403 Forbidden for direct access to hidden namespaces
4. Filters cluster-wide resource listings to exclude resources in hidden namespaces
"""

import base64
import json
import logging
import os
import socket
import ssl
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Set
from urllib.parse import urlparse

import urllib3
from kubernetes import config

from sregym.service.kubeconfig import require_kubeconfig_path

logger = logging.getLogger("all.infra.k8s_proxy")
logger.propagate = True
logger.setLevel(logging.DEBUG)

# Namespaces to hide from agents
HIDDEN_NAMESPACES: Set[str] = {"chaos-mesh", "khaos"}

# Disable SSL warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Upstream connection pool settings
_POOL_MAXSIZE = 20
_CONNECT_TIMEOUT = 10  # seconds
_READ_TIMEOUT = 120  # seconds
_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_UPGRADE_SUBRESOURCES = ("/exec", "/attach", "/portforward")


class KubernetesAPIProxy:
    """Manages the Kubernetes API filtering proxy."""

    def __init__(
        self,
        hidden_namespaces: Set[str] | None = None,
        listen_port: int = 6443,
        kubeconfig_path: str | None = None,
    ):
        self.hidden_namespaces: Set[str] = hidden_namespaces if hidden_namespaces is not None else HIDDEN_NAMESPACES
        self.listen_port = listen_port
        self.server: ThreadingHTTPServer | None = None
        self.server_thread: threading.Thread | None = None
        self._temp_files: list = []
        self._upstream_pool: urllib3.HTTPSConnectionPool | None = None

        # Load Kubernetes config to get upstream API details.
        self.kubeconfig_path = require_kubeconfig_path(kubeconfig_path)

        config.load_kube_config(config_file=self.kubeconfig_path)
        self.api_scheme, self.api_host, self.api_port, self.ca_cert, self.client_cert, self.client_key = (
            self._load_cluster_config(
                kubeconfig_path=self.kubeconfig_path
            )
        )

    def _load_cluster_config(self, kubeconfig_path: str | None = None):
        """Extract API server connection details from kubeconfig."""
        kubeconfig_path = require_kubeconfig_path(kubeconfig_path)

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
        default_port = 443 if (parsed.scheme or "https") == "https" else 80
        api_port = parsed.port or default_port

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

        return parsed.scheme or "https", api_host, api_port, ca_cert, client_cert, client_key

    def _create_upstream_ssl_context(self, cert_files: dict) -> ssl.SSLContext | None:
        """Create an SSL context for direct upgraded upstream connections."""
        if self.api_scheme != "https":
            return None

        context = ssl.create_default_context(cafile=cert_files.get("ca"))
        if cert_files.get("cert") and cert_files.get("key"):
            context.load_cert_chain(certfile=cert_files["cert"], keyfile=cert_files["key"])
        return context

    def _create_temp_cert_files(self):
        """Create temporary files for certificates."""
        files = {}

        if self.ca_cert:
            ca_file = tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False)
            ca_file.write(self.ca_cert)
            ca_file.close()
            files["ca"] = ca_file.name
            self._temp_files.append(ca_file.name)

        if self.client_cert:
            cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False)
            cert_file.write(self.client_cert)
            cert_file.close()
            files["cert"] = cert_file.name
            self._temp_files.append(cert_file.name)

        if self.client_key:
            key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False)
            key_file.write(self.client_key)
            key_file.close()
            files["key"] = key_file.name
            self._temp_files.append(key_file.name)

        return files

    def _create_upstream_pool(self, cert_files: dict) -> urllib3.HTTPConnectionPool | urllib3.HTTPSConnectionPool:
        """Create a connection pool to the upstream Kubernetes API."""
        api_scheme = getattr(self, "api_scheme", "https")
        pool_kwargs = dict(
            host=self.api_host,
            port=self.api_port,
            maxsize=_POOL_MAXSIZE,
            timeout=urllib3.Timeout(connect=_CONNECT_TIMEOUT, read=_READ_TIMEOUT),
            retries=False,
        )
        if api_scheme == "https":
            return urllib3.HTTPSConnectionPool(
                ca_certs=cert_files.get("ca"),
                cert_file=cert_files.get("cert"),
                key_file=cert_files.get("key"),
                **pool_kwargs,
            )
        return urllib3.HTTPConnectionPool(**pool_kwargs)

    def start(self):
        """Start the proxy server in a background thread."""
        cert_files = self._create_temp_cert_files()
        upstream_pool = self._create_upstream_pool(cert_files)
        upstream_ssl_context = self._create_upstream_ssl_context(cert_files)
        self._upstream_pool = upstream_pool

        hidden_namespaces = self.hidden_namespaces
        api_scheme = self.api_scheme
        api_host = self.api_host
        api_port = self.api_port

        class FilteringProxyHandler(BaseHTTPRequestHandler):
            """HTTP request handler that proxies and filters Kubernetes API responses."""

            def log_message(self, format, *args):
                logger.debug(f"Proxy: {format % args}")

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

            def _filter_resource_list(self, data: dict) -> dict:
                """Filter resources in hidden namespaces from list responses."""
                # Handle standard List format
                if "items" in data:
                    data["items"] = [
                        item
                        for item in data["items"]
                        if item.get("metadata", {}).get("namespace") not in hidden_namespaces
                    ]
                # Handle Table format (kubectl's default)
                if "rows" in data:
                    data["rows"] = [
                        row
                        for row in data["rows"]
                        if row.get("object", {}).get("metadata", {}).get("namespace") not in hidden_namespaces
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

                if "/namespaces/" not in path:
                    return "resources"

                return None

            def _is_upgrade_request(self) -> bool:
                """Return True when the client is asking to upgrade the connection."""
                connection_header = self.headers.get("Connection", "")
                upgrade_header = self.headers.get("Upgrade", "")
                wants_upgrade = "upgrade" in connection_header.lower() or bool(upgrade_header)
                return wants_upgrade and any(marker in self.path for marker in _UPGRADE_SUBRESOURCES)

            def _forwardable_headers(self, *, preserve_upgrade: bool = False) -> dict[str, str]:
                """Return request headers safe to forward upstream."""
                connection_tokens = {
                    token.strip().lower()
                    for token in self.headers.get("Connection", "").split(",")
                    if token.strip()
                }
                skip_headers = {"host", "accept-encoding"} | _HOP_BY_HOP_HEADERS | connection_tokens
                if preserve_upgrade:
                    skip_headers -= {"connection", "upgrade"}
                return {k: v for k, v in self.headers.items() if k.lower() not in skip_headers}

            def _filter_json_payload(self, payload: bytes, filter_type: str) -> bytes:
                """Filter standard JSON lists and line-delimited watch streams."""
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    filtered_lines: list[bytes] = []
                    for raw_line in payload.splitlines():
                        if not raw_line.strip():
                            continue
                        try:
                            event = json.loads(raw_line)
                        except json.JSONDecodeError:
                            return payload

                        event_object = event.get("object", {})
                        metadata = event_object.get("metadata", {})
                        if filter_type == "namespaces":
                            if metadata.get("name") in hidden_namespaces:
                                continue
                        elif metadata.get("namespace") in hidden_namespaces:
                            continue
                        filtered_lines.append(json.dumps(event, separators=(",", ":")).encode())

                    if not filtered_lines:
                        return b""

                    suffix = b"\n" if payload.endswith(b"\n") else b""
                    return b"\n".join(filtered_lines) + suffix

                if filter_type == "namespaces":
                    data = self._filter_namespace_list(data)
                elif filter_type == "resources":
                    data = self._filter_resource_list(data)
                return json.dumps(data).encode()

            def _make_upgrade_socket(self) -> socket.socket:
                """Open a direct upstream socket for upgraded streams."""
                upstream_sock = socket.create_connection((api_host, api_port), timeout=_CONNECT_TIMEOUT)
                if api_scheme == "https":
                    assert upstream_ssl_context is not None
                    return upstream_ssl_context.wrap_socket(upstream_sock, server_hostname=api_host)
                return upstream_sock

            def _read_upstream_head(self, upstream_sock: socket.socket) -> tuple[int, str, list[tuple[str, str]], bytes]:
                """Read the upstream status line and headers from a raw socket."""
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = upstream_sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    if len(data) > 65536:
                        raise RuntimeError("Upstream response headers exceeded 64 KiB")

                if b"\r\n\r\n" not in data:
                    raise RuntimeError("Upstream closed connection before sending complete response headers")

                raw_headers, remainder = data.split(b"\r\n\r\n", 1)
                header_lines = raw_headers.decode("iso-8859-1").split("\r\n")
                status_line = header_lines[0]
                parts = status_line.split(" ", 2)
                if len(parts) < 2:
                    raise RuntimeError(f"Malformed upstream status line: {status_line!r}")

                status = int(parts[1])
                reason = parts[2] if len(parts) > 2 else ""
                headers: list[tuple[str, str]] = []
                for line in header_lines[1:]:
                    if not line:
                        continue
                    header, value = line.split(":", 1)
                    headers.append((header, value.lstrip()))
                return status, reason, headers, remainder

            def _relay_bidirectional(
                self,
                left: socket.socket,
                right: socket.socket,
                initial_right_to_left: bytes = b"",
            ):
                """Relay bytes between two sockets until either side closes."""

                def _pump(source: socket.socket, dest: socket.socket):
                    try:
                        while True:
                            chunk = source.recv(65536)
                            if not chunk:
                                break
                            dest.sendall(chunk)
                    except OSError:
                        pass
                    finally:
                        try:
                            dest.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass

                client_to_upstream = threading.Thread(target=_pump, args=(left, right), daemon=True)
                upstream_to_client = threading.Thread(target=_pump, args=(right, left), daemon=True)
                if initial_right_to_left:
                    left.sendall(initial_right_to_left)
                client_to_upstream.start()
                upstream_to_client.start()
                client_to_upstream.join()
                upstream_to_client.join()

            def _proxy_upgrade_request(self, method: str):
                """Tunnel upgraded requests such as kubectl exec/attach websocket streams."""
                path = self.path

                if self._is_hidden_namespace_request(path):
                    self.send_error(403, "Forbidden: Access to this namespace is not allowed")
                    return

                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length) if content_length > 0 else None

                upstream_sock: socket.socket | None = None
                try:
                    upstream_sock = self._make_upgrade_socket()
                    request_lines = [f"{method} {path} HTTP/1.1", f"Host: {api_host}:{api_port}"]
                    for header, value in self._forwardable_headers(preserve_upgrade=True).items():
                        request_lines.append(f"{header}: {value}")
                    request_bytes = ("\r\n".join(request_lines) + "\r\n\r\n").encode("iso-8859-1")
                    upstream_sock.sendall(request_bytes)
                    if body:
                        upstream_sock.sendall(body)

                    status, reason, headers, remainder = self._read_upstream_head(upstream_sock)
                    self.send_response(status, reason)
                    for header, value in headers:
                        if header.lower() not in ("transfer-encoding", "content-length", "content-encoding"):
                            self.send_header(header, value)
                    self.end_headers()
                    self.wfile.flush()

                    if status != 101:
                        response_body = remainder
                        while True:
                            chunk = upstream_sock.recv(65536)
                            if not chunk:
                                break
                            response_body += chunk
                        if response_body:
                            self.wfile.write(response_body)
                            self.wfile.flush()
                        return

                    self.close_connection = True
                    self.connection.settimeout(None)
                    upstream_sock.settimeout(None)
                    self._relay_bidirectional(self.connection, upstream_sock, initial_right_to_left=remainder)
                except Exception as e:
                    logger.error(f"Upgrade proxy error for {method} {path}: {e}")
                    if not getattr(self, "_headers_buffer", None):
                        try:
                            self.send_error(502, f"Bad Gateway: {str(e)}")
                        except BrokenPipeError:
                            pass
                finally:
                    if upstream_sock is not None:
                        try:
                            upstream_sock.close()
                        except OSError:
                            pass

            def _proxy_request(self, method: str):
                """Proxy request to upstream API and filter response."""
                path = self.path

                if self._is_upgrade_request():
                    self._proxy_upgrade_request(method)
                    return

                # Block direct access to hidden namespaces
                if self._is_hidden_namespace_request(path):
                    self.send_error(403, f"Forbidden: Access to this namespace is not allowed")
                    return

                # Read request body if present
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length) if content_length > 0 else None

                # Forward request to upstream via connection pool
                try:
                    headers = self._forwardable_headers()
                    response = upstream_pool.urlopen(
                        method,
                        path,
                        body=body,
                        headers=headers,
                        redirect=False,
                        preload_content=True,
                        decode_content=True,  # urllib3 handles gzip/deflate decompression
                    )

                    response_body = response.data
                    content_type = response.headers.get("Content-Type", "")

                    # Filter JSON responses if needed
                    filter_type = self._should_filter_response(path)
                    if filter_type and response.status == 200 and "application/json" in content_type:
                        response_body = self._filter_json_payload(response_body, filter_type)

                    # Send response to client
                    try:
                        self.send_response(response.status)
                        for header, value in response.headers.items():
                            # Skip headers we're modifying
                            if header.lower() not in ("transfer-encoding", "content-length", "content-encoding"):
                                self.send_header(header, value)
                        self.send_header("Content-Length", str(len(response_body)))
                        self.end_headers()
                        self.wfile.write(response_body)
                    except BrokenPipeError:
                        logger.debug(f"Client disconnected before response was sent for {path}")

                except urllib3.exceptions.TimeoutError as e:
                    logger.warning(f"Upstream timeout for {method} {path}: {e}")
                    try:
                        self.send_error(504, "Gateway Timeout")
                    except BrokenPipeError:
                        pass
                except Exception as e:
                    logger.error(f"Proxy error for {method} {path}: {e}")
                    try:
                        self.send_error(502, f"Bad Gateway: {str(e)}")
                    except BrokenPipeError:
                        pass

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

        # Create and start threaded server (handles concurrent requests)
        self.server = ThreadingHTTPServer(("127.0.0.1", self.listen_port), FilteringProxyHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        logger.info(f"Kubernetes API filtering proxy started on port {self.listen_port}")
        logger.info(f"Hidden namespaces: {self.hidden_namespaces}")

    def stop(self):
        """Stop the proxy server."""
        if self.server:
            # Shutdown in a thread to avoid blocking forever if something is stuck
            t = threading.Thread(target=self.server.shutdown)
            t.start()
            t.join(timeout=2.0)

            if t.is_alive():
                logger.warning("Proxy server shutdown timed out, forcing close")
                try:
                    self.server.server_close()
                except Exception as e:
                    logger.warning(f"Error forcing proxy server close: {e}")

            self.server = None
            self.server_thread = None
            logger.info("Kubernetes API filtering proxy stopped")

        # Close connection pool
        if self._upstream_pool is not None:
            self._upstream_pool.close()
            self._upstream_pool = None

        # Cleanup temp files
        for temp_file in self._temp_files:
            try:
                os.unlink(temp_file)
            except OSError:
                pass
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
            output_path = os.path.join(tempfile.gettempdir(), "sregym-agent-kubeconfig")

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


def start_proxy(hidden_namespaces: Set[str] | None = None, port: int = 16443) -> KubernetesAPIProxy:
    """Start the Kubernetes API filtering proxy."""
    global _proxy_instance
    if _proxy_instance is not None:
        _proxy_instance.stop()
    _proxy_instance = KubernetesAPIProxy(hidden_namespaces=hidden_namespaces, listen_port=port)
    _proxy_instance.start()
    return _proxy_instance


def stop_proxy():
    """Stop the Kubernetes API filtering proxy."""
    global _proxy_instance
    if _proxy_instance is not None:
        _proxy_instance.stop()
        _proxy_instance = None
