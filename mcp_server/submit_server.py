import asyncio
import logging
import traceback

import requests
from fastmcp import FastMCP
from kubernetes import client, config

from clients.stratus.configs.langgraph_tool_configs import LanggraphToolConfig
from clients.stratus.stratus_utils.get_logger import get_logger
from clients.stratus.tools.localization import get_resource_uid
from sregym.service.kubeconfig import require_kubeconfig_path

logger = get_logger()
logger.info("Starting Submission MCP Server")

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


@mcp.tool(name="submit_diagnosis")
def submit_diagnosis(ans: str) -> dict[str, str]:
    """Record your root-cause diagnosis (autonomous mode).

    Args:
        ans: concise description of the root cause you identified.

    Returns:
        Neutral acknowledgement — does not reveal whether the answer is correct.
    """
    cfg = LanggraphToolConfig()
    url = cfg.benchmark_submit_url.replace("/submit", "/submit_diagnosis")
    headers = {"Content-Type": "application/json"}
    payload = {"solution": ans}
    try:
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"[submit_diagnosis] status: {response.status_code}")
        return {"status": str(response.status_code), "text": "Diagnosis recorded."}
    except Exception as e:
        logger.error(f"[submit_diagnosis] failed: {e}")
        return {"status": "N/A", "text": f"[submit_diagnosis] failed: {e}"}


@mcp.tool(name="submit_mitigation")
def submit_mitigation(ans: str) -> dict[str, str]:
    """Record the mitigation actions you took (autonomous mode).

    Args:
        ans: short summary of the changes you made to restore the cluster.

    Returns:
        Neutral acknowledgement — does not reveal whether mitigation succeeded.
    """
    cfg = LanggraphToolConfig()
    url = cfg.benchmark_submit_url.replace("/submit", "/submit_mitigation")
    headers = {"Content-Type": "application/json"}
    payload = {"solution": ans}
    try:
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"[submit_mitigation] status: {response.status_code}")
        return {"status": str(response.status_code), "text": "Mitigation recorded."}
    except Exception as e:
        logger.error(f"[submit_mitigation] failed: {e}")
        return {"status": "N/A", "text": f"[submit_mitigation] failed: {e}"}


@mcp.tool(name="submit_done")
def submit_done() -> dict:
    """Signal that your investigation is complete (autonomous mode).

    Call this **once**, after you have submitted all diagnoses and exactly one
    mitigation. On this call the benchmark stops the TTL clock, grades the
    diagnosis submissions you accumulated, and returns either a neutral
    completion payload or rich grading feedback depending on
    ``SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK``.

    After submit_done returns, further submit_diagnosis and submit_mitigation
    calls are rejected. Only call store_incident after this.

    Returns:
        dict containing at minimum `status`, `ttl`, `ttm`, and
        `num_diagnosis_submissions`. When feedback is enabled it also includes
        `diagnosis`, `mitigation`, `ground_truth_diagnosis`, and
        `diagnosis_submissions`.
    """
    cfg = LanggraphToolConfig()
    url = cfg.benchmark_submit_url.replace("/submit", "/submit_done")
    try:
        response = requests.post(url, json={}, headers={"Content-Type": "application/json"})
        logger.info(f"[submit_done] status: {response.status_code}")
        if response.status_code >= 400:
            return {"status": str(response.status_code), "text": response.text}
        return response.json()
    except Exception as e:
        logger.error(f"[submit_done] failed: {e}")
        return {"status": "N/A", "text": f"[submit_done] failed: {e}"}


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
