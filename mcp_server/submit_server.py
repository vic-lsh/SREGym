import requests
from fastmcp import FastMCP

from clients.stratus.configs.langgraph_tool_configs import LanggraphToolConfig
from clients.stratus.stratus_utils.get_logger import get_logger

logger = get_logger()
logger.info("Starting Submission MCP Server")

langgraph_tool_config = LanggraphToolConfig()

mcp = FastMCP("Submission MCP Server")


@mcp.tool(name="submit")
def submit(ans: str | list[str]) -> dict[str, str]:
    """Submit task result to benchmark.

    Args:
        ans: task result that the agent submits. May be a single string
            (a single diagnosis/mitigation answer) or a list of candidate
            diagnoses. When a list is provided, the benchmark grades the
            submission as successful if its ground-truth matches *any*
            candidate in the list — useful when the cluster exhibits
            multiple plausible faults simultaneously.

    Returns:
        dict[str]: http response code and response text of benchmark submission server
    """

    logger.info("[submit_mcp] submit mcp called")
    # FIXME: reference url from config file, remove hard coding
    url = langgraph_tool_config.benchmark_submit_url
    headers = {"Content-Type": "application/json"}
    payload = {"solution": ans}

    try:
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"[submit_mcp] Response status: {response.status_code}, text: {response.text}")
        return {"status": str(response.status_code), "text": str(response.text)}

    except Exception as e:
        logger.error(f"[submit_mcp] HTTP submission failed: {e}")
        return {"status": "N/A", "text": f"[submit_mcp] HTTP submission failed: {e}"}
