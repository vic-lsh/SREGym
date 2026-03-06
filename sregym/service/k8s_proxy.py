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
        self.api_host, self.api_port, self.ca_cert, self.client_cert, self.client_key = self._load_cluster_config(
            kubeconfig_path=self.kubeconfig_path
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

    def _create_upstream_pool(self, cert_files: dict) -> urllib3.HTTPSConnectionPool:
        """Create a connection pool to the upstream Kubernetes API."""
        return urllib3.HTTPSConnectionPool(
            self.api_host,
            self.api_port,
            maxsize=_POOL_MAXSIZE,
            timeout=urllib3.Timeout(connect=_CONNECT_TIMEOUT, read=_READ_TIMEOUT),
            retries=False,
            ca_certs=cert_files.get("ca"),
            cert_file=cert_files.get("cert"),
            key_file=cert_files.get("key"),
        )

    def start(self):
        """Start the proxy server in a background thread."""
        cert_files = self._create_temp_cert_files()
        upstream_pool = self._create_upstream_pool(cert_files)
        self._upstream_pool = upstream_pool

        hidden_namespaces = self.hidden_namespaces

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
                    self.send_error(403, f"Forbidden: Access to this namespace is not allowed")
                    return

                # Read request body if present
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length) if content_length > 0 else None

                # Forward request to upstream via connection pool
                try:
                    # Forward headers (except Host and Accept-Encoding so urllib3 controls compression)
                    headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "accept-encoding")}
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
                        try:
                            data = json.loads(response_body)
                            if filter_type == "namespaces":
                                data = self._filter_namespace_list(data)
                            elif filter_type == "resources":
                                data = self._filter_resource_list(data)
                            response_body = json.dumps(data).encode()
                        except json.JSONDecodeError:
                            pass  # Not valid JSON, pass through as-is

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
