"""
Unit tests for the Kubernetes API Filtering Proxy.

These tests mock the upstream connection pool and kubeconfig loading
so they can run without a real Kubernetes cluster.
"""

import http.client
import json
import socket
import time
from contextlib import contextmanager
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest
import urllib3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_free_port() -> int:
    """Return an unused TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_mock_response(status: int, body, content_type: str = "application/json"):
    """Build a mock urllib3 response object."""
    if isinstance(body, dict) or isinstance(body, list):
        raw = json.dumps(body).encode()
    elif isinstance(body, str):
        raw = body.encode()
    else:
        raw = body

    resp = MagicMock()
    resp.status = status
    resp.data = raw
    # headers must support .get() and .items()
    resp.headers = {"Content-Type": content_type}
    return resp


def _make_proxy(port: int, mock_pool, hidden_namespaces=None):
    """Create a KubernetesAPIProxy with all external I/O mocked."""
    from sregym.service.k8s_proxy import KubernetesAPIProxy

    proxy = KubernetesAPIProxy.__new__(KubernetesAPIProxy)
    proxy.hidden_namespaces = hidden_namespaces if hidden_namespaces is not None else {"chaos-mesh", "khaos"}
    proxy.listen_port = port
    proxy.server = None
    proxy.server_thread = None
    proxy._temp_files = []
    proxy._upstream_pool = None
    proxy.api_host = "fake-k8s-api"
    proxy.api_port = 6443
    proxy.ca_cert = None
    proxy.client_cert = None
    proxy.client_key = None
    proxy.kubeconfig_path = "/fake/kubeconfig"

    # Bypass cert-file creation and pool construction
    proxy._create_temp_cert_files = lambda: {}
    proxy._create_upstream_pool = lambda cert_files: mock_pool

    return proxy


@contextmanager
def _running_proxy(mock_pool, hidden_namespaces=None):
    """Context manager that starts a proxy and stops it on exit."""
    port = _find_free_port()
    proxy = _make_proxy(port, mock_pool, hidden_namespaces)
    proxy.start()
    # Give the server thread a moment to bind
    time.sleep(0.05)
    try:
        yield proxy
    finally:
        proxy.stop()


def _http_request(port: int, method: str, path: str, body: bytes | None = None):
    """Send an HTTP request to the proxy and return (status, body_bytes)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {}
    if body is not None:
        headers["Content-Length"] = str(len(body))
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


# ---------------------------------------------------------------------------
# Tests: namespace / resource filtering
# ---------------------------------------------------------------------------

class TestFilteringLogic:

    def test_namespace_list_filters_hidden_from_items(self):
        """Hidden namespaces must be removed from /api/v1/namespaces items."""
        upstream_body = {
            "apiVersion": "v1",
            "kind": "NamespaceList",
            "items": [
                {"metadata": {"name": "default"}},
                {"metadata": {"name": "chaos-mesh"}},   # should be removed
                {"metadata": {"name": "khaos"}},         # should be removed
                {"metadata": {"name": "astronomy-shop"}},
            ],
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/namespaces")

        assert status == 200
        data = json.loads(body)
        names = [item["metadata"]["name"] for item in data["items"]]
        assert "default" in names
        assert "astronomy-shop" in names
        assert "chaos-mesh" not in names
        assert "khaos" not in names

    def test_namespace_list_filters_hidden_from_table_rows(self):
        """Table-format namespace responses (kubectl default) are also filtered."""
        upstream_body = {
            "kind": "Table",
            "rows": [
                {"object": {"metadata": {"name": "default"}}},
                {"object": {"metadata": {"name": "chaos-mesh"}}},
                {"object": {"metadata": {"name": "khaos"}}},
            ],
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/namespaces")

        assert status == 200
        data = json.loads(body)
        row_names = [r["object"]["metadata"]["name"] for r in data["rows"]]
        assert "default" in row_names
        assert "chaos-mesh" not in row_names
        assert "khaos" not in row_names

    def test_resource_list_filters_resources_in_hidden_namespaces(self):
        """Cluster-wide pod listing must exclude pods from hidden namespaces."""
        upstream_body = {
            "apiVersion": "v1",
            "kind": "PodList",
            "items": [
                {"metadata": {"name": "app-pod", "namespace": "default"}},
                {"metadata": {"name": "chaos-pod", "namespace": "chaos-mesh"}},
                {"metadata": {"name": "khaos-pod", "namespace": "khaos"}},
            ],
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/pods")

        assert status == 200
        data = json.loads(body)
        ns_list = [item["metadata"]["namespace"] for item in data["items"]]
        assert "default" in ns_list
        assert "chaos-mesh" not in ns_list
        assert "khaos" not in ns_list

    def test_resource_table_format_filters_hidden_namespaces(self):
        """Table-format cluster-wide resource listings are also filtered."""
        upstream_body = {
            "kind": "Table",
            "rows": [
                {"object": {"metadata": {"name": "app-svc", "namespace": "default"}}},
                {"object": {"metadata": {"name": "chaos-svc", "namespace": "chaos-mesh"}}},
            ],
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/services")

        assert status == 200
        data = json.loads(body)
        ns_list = [r["object"]["metadata"]["namespace"] for r in data["rows"]]
        assert "default" in ns_list
        assert "chaos-mesh" not in ns_list

    def test_non_filtered_endpoint_passes_through_unmodified(self):
        """Responses for non-filtered paths are forwarded as-is."""
        upstream_body = {"apiVersion": "v1", "kind": "APIVersions", "versions": ["v1"]}
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api")

        assert status == 200
        data = json.loads(body)
        assert data["kind"] == "APIVersions"
        # Pool was called once and response was not modified
        assert mock_pool.urlopen.call_count == 1

    def test_non_json_response_passed_through_unmodified(self):
        """Non-JSON responses on filtered paths are forwarded unchanged."""
        raw = b"not-json-content"
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, raw, content_type="text/plain")

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/namespaces")

        assert status == 200
        assert body == raw

    def test_deployments_cluster_wide_filtered(self):
        """Cluster-wide deployment listing filters hidden namespaces."""
        upstream_body = {
            "items": [
                {"metadata": {"name": "my-app", "namespace": "default"}},
                {"metadata": {"name": "chaos-operator", "namespace": "chaos-mesh"}},
            ]
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/apis/apps/v1/deployments")

        assert status == 200
        data = json.loads(body)
        names = [i["metadata"]["name"] for i in data["items"]]
        assert "my-app" in names
        assert "chaos-operator" not in names

    def test_namespaced_endpoint_not_filtered(self):
        """Requests scoped to a specific (non-hidden) namespace are not filtered."""
        upstream_body = {
            "items": [
                {"metadata": {"name": "pod-a", "namespace": "default"}},
            ]
        }
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(
                proxy.listen_port, "GET", "/api/v1/namespaces/default/pods"
            )

        assert status == 200
        data = json.loads(body)
        assert len(data["items"]) == 1

    def test_non_200_response_not_filtered(self):
        """Non-200 responses (e.g. 404) are passed through without filtering."""
        upstream_body = {"kind": "Status", "code": 404, "message": "not found"}
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(404, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/namespaces")

        assert status == 404
        data = json.loads(body)
        assert data["code"] == 404


# ---------------------------------------------------------------------------
# Tests: hidden namespace blocking (403)
# ---------------------------------------------------------------------------

class TestHiddenNamespaceBlocking:

    def test_direct_access_to_hidden_namespace_blocked(self):
        """GET /api/v1/namespaces/chaos-mesh must return 403."""
        mock_pool = MagicMock()  # should never be called

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(proxy.listen_port, "GET", "/api/v1/namespaces/chaos-mesh")

        assert status == 403
        mock_pool.urlopen.assert_not_called()

    def test_namespaced_resource_in_hidden_namespace_blocked(self):
        """Access to resources inside a hidden namespace returns 403."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(
                proxy.listen_port, "GET", "/api/v1/namespaces/khaos/pods"
            )

        assert status == 403
        mock_pool.urlopen.assert_not_called()

    def test_apis_group_hidden_namespace_blocked(self):
        """Hidden namespace access via /apis/{group}/{version}/namespaces/... blocked."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(
                proxy.listen_port,
                "GET",
                "/apis/apps/v1/namespaces/chaos-mesh/deployments",
            )

        assert status == 403

    def test_non_hidden_namespace_allowed(self):
        """Non-hidden namespaces are not blocked."""
        upstream_body = {"items": [{"metadata": {"name": "my-pod", "namespace": "default"}}]}
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(
                proxy.listen_port, "GET", "/api/v1/namespaces/default/pods"
            )

        assert status == 200
        mock_pool.urlopen.assert_called_once()

    def test_custom_hidden_namespace_blocked(self):
        """Custom hidden namespaces set at construction time are also blocked."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool, hidden_namespaces={"my-secret-ns"}) as proxy:
            status, _ = _http_request(
                proxy.listen_port, "GET", "/api/v1/namespaces/my-secret-ns"
            )

        assert status == 403
        mock_pool.urlopen.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: HTTP method forwarding
# ---------------------------------------------------------------------------

class TestMethodForwarding:

    @pytest.mark.parametrize("method", ["GET", "POST", "PUT", "DELETE", "PATCH"])
    def test_http_method_forwarded(self, method):
        """All HTTP methods are proxied to the upstream pool."""
        upstream_body = {"ok": True}
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, upstream_body)

        with _running_proxy(mock_pool) as proxy:
            status, body = _http_request(proxy.listen_port, method, "/api/v1/nodes")

        assert status == 200
        call_args = mock_pool.urlopen.call_args
        assert call_args[0][0] == method


# ---------------------------------------------------------------------------
# Tests: concurrency (ThreadingHTTPServer)
# ---------------------------------------------------------------------------

class TestConcurrency:

    def test_concurrent_requests_processed_in_parallel(self):
        """
        With ThreadingHTTPServer, N simultaneous requests each taking ~delay seconds
        should complete in total time close to delay (not N * delay).
        """
        delay = 0.3  # seconds per request
        n_requests = 5
        expected_max = delay * 2  # allow generous margin for scheduling

        def slow_response(*args, **kwargs):
            time.sleep(delay)
            return _make_mock_response(200, {"ok": True})

        mock_pool = MagicMock()
        mock_pool.urlopen.side_effect = slow_response

        results = []

        def do_request(port):
            status, _ = _http_request(port, "GET", "/api")
            results.append(status)

        with _running_proxy(mock_pool) as proxy:
            threads = [Thread(target=do_request, args=(proxy.listen_port,)) for _ in range(n_requests)]
            t0 = time.monotonic()
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            elapsed = time.monotonic() - t0

        assert len(results) == n_requests, "All requests should complete"
        assert all(s == 200 for s in results), "All requests should succeed"
        assert elapsed < expected_max, (
            f"Requests took {elapsed:.2f}s — expected concurrent execution (< {expected_max}s)"
        )


# ---------------------------------------------------------------------------
# Tests: connection pool lifecycle
# ---------------------------------------------------------------------------

class TestConnectionPoolLifecycle:

    def test_upstream_pool_set_after_start(self):
        """_upstream_pool should be set to the mock pool after start()."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool) as proxy:
            assert proxy._upstream_pool is mock_pool

    def test_pool_closed_on_stop(self):
        """stop() must call close() on the upstream connection pool."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool) as proxy:
            pass  # _running_proxy calls stop() on exit

        mock_pool.close.assert_called_once()

    def test_pool_set_to_none_after_stop(self):
        """_upstream_pool should be None after stop()."""
        mock_pool = MagicMock()

        with _running_proxy(mock_pool) as proxy:
            pass

        assert proxy._upstream_pool is None

    def test_create_upstream_pool_returns_https_pool(self):
        """_create_upstream_pool should return an HTTPSConnectionPool."""
        from sregym.service.k8s_proxy import KubernetesAPIProxy

        proxy = KubernetesAPIProxy.__new__(KubernetesAPIProxy)
        proxy.api_host = "example.com"
        proxy.api_port = 6443

        pool = proxy._create_upstream_pool({})  # no cert files
        try:
            assert isinstance(pool, urllib3.HTTPSConnectionPool)
            assert pool.host == "example.com"
            assert pool.port == 6443
        finally:
            pool.close()


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def test_upstream_timeout_returns_504(self):
        """When the upstream pool raises TimeoutError, proxy must respond 504."""
        mock_pool = MagicMock()
        mock_pool.urlopen.side_effect = urllib3.exceptions.TimeoutError("timed out")

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(proxy.listen_port, "GET", "/api/v1/nodes")

        assert status == 504

    def test_upstream_error_returns_502(self):
        """Generic upstream errors must return 502 Bad Gateway."""
        mock_pool = MagicMock()
        mock_pool.urlopen.side_effect = Exception("connection refused")

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(proxy.listen_port, "GET", "/api/v1/nodes")

        assert status == 502

    def test_broken_pipe_on_send_does_not_crash_server(self):
        """
        A BrokenPipeError while writing the response must not crash the server.
        Subsequent requests must still succeed.
        """
        call_count = [0]
        delay = 0.2

        def maybe_slow(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # Slow first response so the client may have disconnected
                time.sleep(delay)
            return _make_mock_response(200, {"ok": True})

        mock_pool = MagicMock()
        mock_pool.urlopen.side_effect = maybe_slow

        with _running_proxy(mock_pool) as proxy:
            # First request: client disconnects immediately after sending
            try:
                conn = http.client.HTTPConnection("127.0.0.1", proxy.listen_port, timeout=0.05)
                conn.request("GET", "/api/v1/nodes")
                conn.getresponse()
            except Exception:
                pass  # timeout / reset expected
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            # Wait for first request to finish processing
            time.sleep(delay + 0.1)

            # Second request must succeed
            status, body = _http_request(proxy.listen_port, "GET", "/api/v1/nodes")

        assert status == 200


# ---------------------------------------------------------------------------
# Tests: proxy lifecycle
# ---------------------------------------------------------------------------

class TestProxyLifecycle:

    def test_start_uses_threading_http_server(self):
        """start() must create a ThreadingHTTPServer, not a plain HTTPServer."""
        from http.server import ThreadingHTTPServer
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, {})

        with _running_proxy(mock_pool) as proxy:
            assert isinstance(proxy.server, ThreadingHTTPServer)

    def test_stop_sets_server_to_none(self):
        """After stop(), server attribute must be None."""
        mock_pool = MagicMock()
        with _running_proxy(mock_pool) as proxy:
            pass
        assert proxy.server is None

    def test_proxy_responds_after_start(self):
        """A started proxy must accept HTTP connections."""
        mock_pool = MagicMock()
        mock_pool.urlopen.return_value = _make_mock_response(200, {"ok": True})

        with _running_proxy(mock_pool) as proxy:
            status, _ = _http_request(proxy.listen_port, "GET", "/api")

        assert status == 200
