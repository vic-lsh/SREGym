import contextlib
import json
import shlex
import time
import uuid

from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.base import Oracle


class DNSResolutionMitigationOracle(Oracle):
    """Every Running pod's DNS configuration resolves every Service name in the namespace.

    Checking every pod (not only the first) matters during a stalled rollout: the previous
    ReplicaSet's pod still resolves while the pod carrying the fault cannot start, and a
    first-pod check passed with the fault in place (live composition screen, #150).
    """

    importance = 1.0
    concurrent_safe = False

    def _probe(self, source, services: list[str]) -> bool:
        """Resolve from an owned tool image using the source pod's DNS settings.

        Application images may be distroless. Probe failures to start are infra
        errors; only a terminated resolver process supplies a DNS verdict.
        """
        namespace = self.problem.namespace
        api = self.problem.kubectl.core_v1_api
        name = f"sregym-dns-{uuid.uuid4().hex[:12]}"
        checks = " && ".join(f"nslookup {shlex.quote(f'{svc}.{namespace}.svc.cluster.local')}" for svc in services)
        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=name, namespace=namespace, labels={**(source.metadata.labels or {}), "sregym-oracle": "dns"}
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                automount_service_account_token=False,
                dns_policy=source.spec.dns_policy,
                dns_config=source.spec.dns_config,
                node_name=source.spec.node_name,
                host_network=source.spec.host_network,
                containers=[
                    client.V1Container(
                        name="dns",
                        image="busybox:1.36.1",
                        command=["sh", "-c", checks],
                        # Copy selectors so DNS-blocking NetworkPolicies apply,
                        # but never join the application's ready Service endpoints.
                        readiness_probe=client.V1Probe(_exec=client.V1ExecAction(command=["false"])),
                        resources=client.V1ResourceRequirements(
                            requests={"cpu": "10m", "memory": "8Mi"},
                            limits={"cpu": "100m", "memory": "32Mi"},
                        ),
                    )
                ],
            ),
        )
        try:
            api.create_namespaced_pod(namespace=namespace, body=pod)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                current = api.read_namespaced_pod(name=name, namespace=namespace)
                if current.status.phase == "Succeeded":
                    return True
                if current.status.phase == "Failed":
                    statuses = current.status.container_statuses or []
                    if not statuses or not statuses[0].state.terminated:
                        raise RuntimeError("DNS probe failed without a resolver exit status")
                    exit_code = statuses[0].state.terminated.exit_code
                    if exit_code != 1:
                        raise RuntimeError(f"DNS probe terminated with infrastructure exit code {exit_code}")
                    return False
                time.sleep(1)
            raise RuntimeError("DNS oracle probe did not terminate within 90s")
        finally:
            with contextlib.suppress(ApiException):
                api.delete_namespaced_pod(name=name, namespace=namespace, grace_period_seconds=0)

    def evaluate(self) -> dict:
        print("== DNS Resolution Mitigation Check ==")

        kubectl = self.problem.kubectl
        namespace = self.problem.namespace
        faulty_service = self.problem.faulty_service

        service_names = [svc.metadata.name for svc in kubectl.list_services(namespace).items]

        if faulty_service is None:
            faulty_service = service_names[0]

        # Get the service's selector
        command = f"kubectl get service {faulty_service} -n {namespace} -o jsonpath='{{.spec.selector}}'"
        selector_output = kubectl.exec_command(command).strip()

        # Parse selector to label query
        try:
            selector_dict = json.loads(selector_output.strip("'"))
        except json.JSONDecodeError:
            print(f"❌ Cannot read the selector of service {faulty_service}: {selector_output[:200]}")
            return {"success": False}
        label_selector = ",".join(f"{k}={v}" for k, v in selector_dict.items())

        # Running pods only: a Pending pod cannot be exec'd and is the collateral check's concern.
        command = (
            f"kubectl get pods -n {namespace} -l {label_selector} --field-selector=status.phase=Running "
            "-o jsonpath='{.items[*].metadata.name}'"
        )
        pod_names = kubectl.exec_command(command).strip().strip("'").split()

        if not pod_names:
            print("❌ No running pod found for the faulty service(s).")
            return {"success": False}

        for pod in pod_names:
            source = kubectl.core_v1_api.read_namespaced_pod(name=pod, namespace=namespace)
            if not self._probe(source, service_names):
                print(f"[FAIL] DNS settings of {pod} cannot resolve every namespace service")
                return {"success": False}

        return {"success": True}
