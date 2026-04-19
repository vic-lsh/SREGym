import os
import traceback

import requests
from fastmcp import FastMCP
from kubernetes import client, config

from clients.stratus.configs.langgraph_tool_configs import LanggraphToolConfig
from clients.stratus.stratus_utils.get_logger import get_logger
from sregym.service.kubeconfig import require_kubeconfig_path

logger = get_logger()
logger.info("Starting Submission MCP Server")

mcp = FastMCP("Submission MCP Server")

# Autonomous-submit mode: when the env var is set at module load, register
# per-stage submit tools that never leak the oracle verdict back to the
# agent. The agent is expected to self-verify via the cluster before
# submitting. Reading at import time is intentional — FastMCP's
# ``@mcp.tool`` decorators bind tool identity at module load, so a runtime
# toggle would not reach the client. See
# ``sregym_agents/experiment_config.py::config_to_env`` for how the env
# var is propagated from the experiment TOML.
_AUTONOMOUS_SUBMIT = os.getenv("SREGYM_AUTONOMOUS_SUBMIT", "").strip() == "1"


def _post_stage_submission(stage: str, ans: str | list[str]) -> dict[str, str]:
    """POST to the conductor's /submit_stage endpoint and return a neutral ack.

    Must not surface oracle verdicts or grading fields in its return — the
    agent is expected to self-verify via the cluster, so any leak would
    undermine the autonomous-mode design.
    """
    langgraph_tool_config = LanggraphToolConfig()
    base = langgraph_tool_config.benchmark_submit_url.rsplit("/", 1)[0]
    url = f"{base}/submit_stage"
    headers = {"Content-Type": "application/json"}
    payload = {"solution": ans, "stage": stage}

    try:
        response = requests.post(url, json=payload, headers=headers)
    except Exception as e:
        logger.error(f"[submit_mcp] autonomous submission HTTP call failed: {e}")
        return {"status": "error", "text": "submission HTTP call failed"}

    logger.info(f"[submit_mcp] autonomous submit status: {response.status_code}")
    if response.status_code == 200:
        return {"status": "recorded"}
    return {"status": "error", "text": f"HTTP {response.status_code}"}


if _AUTONOMOUS_SUBMIT:

    @mcp.tool(name="submit_diagnosis")
    def submit_diagnosis(ans: str | list[str]) -> dict[str, str]:
        """Record your root-cause diagnosis for the current problem.

        Args:
            ans: A concise one-line description of the fault's root cause,
                or a list of candidate root-cause descriptions.

        Returns:
            A neutral acknowledgement ("recorded" on success, "error"
            otherwise). Deliberately does not tell you whether your
            answer matches the benchmark's expected root cause — verify
            via the cluster itself.
        """
        logger.info("[submit_mcp] submit_diagnosis called")
        return _post_stage_submission("diagnosis", ans)

    @mcp.tool(name="submit_mitigation")
    def submit_mitigation(ans: str | list[str]) -> dict[str, str]:
        """Record the mitigation actions you took for the current problem.

        Args:
            ans: A short summary of the concrete changes you made
                (deployment restarts, config fixes, etc.).

        Returns:
            A neutral acknowledgement ("recorded" on success, "error"
            otherwise). The mitigation oracle judges the cluster's final
            state, not the summary text — verify the cluster is healthy
            before calling this.
        """
        logger.info("[submit_mcp] submit_mitigation called")
        return _post_stage_submission("mitigation", ans)

else:

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
        langgraph_tool_config = LanggraphToolConfig()

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


@mcp.tool(name="localization")
async def localization(
    resource_type: str,
    resource_name: str,
    namespace: str,
) -> dict[str, str]:
    """Retrieve the UID of a specified Kubernetes resource."""
    config.load_kube_config(config_file=require_kubeconfig_path())
    try:
        cmd = [
            "kubectl",
            "get",
            resource_type,
            resource_name,
            "-n",
            namespace,
            "-o",
            "jsonpath={.metadata.uid}",
        ]
        logger.info(f"[localization_mcp] Running command: {' '.join(cmd)}")
        if resource_type.lower() == "pod":
            api = client.CoreV1Api()
            obj = api.read_namespaced_pod(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "service":
            api = client.CoreV1Api()
            obj = api.read_namespaced_service(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "deployment":
            api = client.AppsV1Api()
            obj = api.read_namespaced_deployment(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "statefulset":
            api = client.AppsV1Api()
            obj = api.read_namespaced_stateful_set(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "persistentvolumeclaim":
            api = client.CoreV1Api()
            obj = api.read_namespaced_persistent_volume_claim(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "persistentvolume":
            api = client.CoreV1Api()
            obj = api.read_persistent_volume(name=resource_name)
        elif resource_type.lower() == "configmap":
            api = client.CoreV1Api()
            obj = api.read_namespaced_config_map(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "replicaset":
            api = client.AppsV1Api()
            obj = api.read_namespaced_replica_set(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "memoryquota":
            api = client.CoreV1Api()
            obj = api.read_namespaced_resource_quota(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "ingress":
            api = client.NetworkingV1Api()
            obj = api.read_namespaced_ingress(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "networkpolicy":
            api = client.NetworkingV1Api()
            obj = api.read_namespaced_network_policy(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "tidbcluster":
            api = client.CustomObjectsApi()
            obj = api.read_namespaced_custom_object(
                group="pingcap.com", version="v1alpha1", namespace=namespace, plural="tidbclusters", name=resource_name
            )
        elif resource_type.lower() == "job":
            api = client.BatchV1Api()
            obj = api.read_namespaced_job(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "daemonset":
            api = client.AppsV1Api()
            obj = api.read_namespaced_daemon_set(name=resource_name, namespace=namespace)
        elif resource_type.lower() == "clusterrole":
            api = client.RbacAuthorizationV1Api()
            obj = api.read_cluster_role(name=resource_name)
        elif resource_type.lower() == "clusterrolebinding":
            api = client.RbacAuthorizationV1Api()
            obj = api.read_cluster_role_binding(name=resource_name)
        else:
            err_msg = f"Unsupported resource type: {resource_type}"
            logger.error(f"[localization_mcp] {err_msg}")
            return {"uid": f"Error: {err_msg}"}
        uid = obj.metadata.uid
        logger.info(f"[localization_mcp] Retrieved UID using Kubernetes client: {uid}")
        return {"uid": uid}
    except Exception as e:
        logger.error(f"[localization_mcp] Exception occurred: {e}")
        logger.error(traceback.format_exc())
        return {"uid": f"Exception: {e}"}
