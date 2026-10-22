from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.base import Oracle


class ServiceEndpointMitigationOracle(Oracle):
    """
    Oracle that verifies every Service has at least one ready endpoint.
    """

    importance = 1.0

    # Services that are part of the observability stack, not the application
    # under test. Missing endpoints on these are not a mitigation failure.
    INFRA_PREFIXES = (
        "grafana",
        "prometheus",
        "alertmanager",
        "jaeger",
        "otel-collector",
        "opensearch",
        "blackbox",
        "observe",
    )

    def evaluate(self) -> dict:
        print("== Service Endpoints Evaluation ==")

        kubectl = self.problem.kubectl
        namespace = self.problem.namespace

        services_to_check = [svc.metadata.name for svc in kubectl.list_services(namespace).items]

        for svc_name in services_to_check:
            if any(svc_name.startswith(p) for p in self.INFRA_PREFIXES):
                continue
            try:
                ep = kubectl.core_v1_api.read_namespaced_endpoints(svc_name, namespace)
                has_ready = any(subset.addresses for subset in (ep.subsets or []))
                if not has_ready:
                    print(f"❌ Service {svc_name} has NO ready endpoints")
                    return {"success": False}
            except ApiException as e:
                if e.status == 404:
                    print(f"⚠️  Service {svc_name} has no Endpoints resource (skipping)")
                    continue
                print(f"❌ Error retrieving endpoints for service {svc_name}: {e}")
                return {"success": False}
            except Exception as e:
                print(f"❌ Error retrieving endpoints for service {svc_name}: {e}")
                return {"success": False}

        print("[✅] All checked services have ready endpoints.")
        return {"success": True}
