import logging

import uvicorn
from fastmcp.server.http import create_sse_app
from starlette.applications import Starlette
from starlette.routing import Mount

from mcp_server.configs.load_all_cfg import mcp_server_cfg
from mcp_server.configs.mcp_tool_cfg import McpToolCfg
from mcp_server.jaeger_server import create_jaeger_mcp, mcp as observability_mcp
from mcp_server.kubectl_mcp_tools import create_kubectl_mcp, kubectl_mcp
from mcp_server.prometheus_server import create_prometheus_mcp, mcp as prometheus_mcp
from mcp_server.submit_server import create_submit_mcp, mcp as submit_mcp

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = Starlette(
    routes=[
        Mount("/kubectl_mcp_tools", app=create_sse_app(kubectl_mcp, "/messages/", "/sse")),
        Mount("/jaeger", app=create_sse_app(observability_mcp, "/messages/", "/sse")),
        Mount("/prometheus", app=create_sse_app(prometheus_mcp, "/messages/", "/sse")),
        Mount("/submit", app=create_sse_app(submit_mcp, "/messages/", "/sse")),
    ]
)


def create_mcp_app(tool_cfg: McpToolCfg | None = None) -> Starlette:
    if tool_cfg is None:
        return app

    kubectl_instance = create_kubectl_mcp()
    jaeger_instance = create_jaeger_mcp(tool_cfg=tool_cfg)
    prometheus_instance = create_prometheus_mcp(tool_cfg=tool_cfg)
    submit_instance = create_submit_mcp(tool_cfg=tool_cfg)

    return Starlette(
        routes=[
            Mount("/kubectl_mcp_tools", app=create_sse_app(kubectl_instance, "/messages/", "/sse")),
            Mount("/jaeger", app=create_sse_app(jaeger_instance, "/messages/", "/sse")),
            Mount("/prometheus", app=create_sse_app(prometheus_instance, "/messages/", "/sse")),
            Mount("/submit", app=create_sse_app(submit_instance, "/messages/", "/sse")),
        ]
    )

if __name__ == "__main__":
    port = mcp_server_cfg.mcp_server_port
    host = "0.0.0.0" if mcp_server_cfg.expose_server else "127.0.0.1"
    logger.info("Starting SREGym MCP Server")
    uvicorn.run(app, host=host, port=port)
