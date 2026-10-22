import json

from sregym.conductor.oracles.base import Oracle


def resolved(getent_output: str, fqdn: str) -> bool:
    """True iff `getent hosts <fqdn>` printed a hosts line for `fqdn`.

    `KubeCtl.exec_command` returns stderr when the command fails, so the output alone must
    prove success: a kubectl error ("container not found", "pod not found") or an empty
    getent result never contains the name as a whitespace-separated token.
    """
    return fqdn in getent_output.split()


class DNSResolutionMitigationOracle(Oracle):
    """Every Running pod of the faulty service resolves every Service name in the namespace.

    Checking every pod (not only the first) matters during a stalled rollout: the previous
    ReplicaSet's pod still resolves while the pod carrying the fault cannot start, and a
    first-pod check passed with the fault in place (live composition screen, #150).
    """

    importance = 1.0

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
            for svc in service_names:
                fqdn = f"{svc}.{namespace}.svc.cluster.local"
                output = kubectl.exec_command(f"kubectl exec -n {namespace} {pod} -- getent hosts {fqdn}")
                if not resolved(output, fqdn):
                    print(
                        f"[❌] Faulty Service: {faulty_service} | pod {pod} cannot resolve {svc}: {output.strip()[:200]}"
                    )
                    return {"success": False}
            print(f"[✅] All {len(service_names)} service names resolved inside pod {pod}")

        return {"success": True}
