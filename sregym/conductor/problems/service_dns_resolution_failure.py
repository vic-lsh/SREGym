from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.dns_resolution_mitigation import DNSResolutionMitigationOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

# Maps dns_failure_layer values to their configuration.
# - uses_rcode: whether dns_response_code is meaningful for this layer
# - description_fn: callable(self) -> str producing the root cause description
_DNS_FAILURE_LAYERS = {
    "coredns_template": {
        "uses_rcode": True,
        "description_fn": lambda self: (
            f"CoreDNS is configured with a {self.dns_response_code} template for "
            f"`{self.faulty_service}.{self.namespace}.svc.cluster.local`, "
            "so in-cluster lookups for this service name fail at DNS resolution time. "
            "Dependent services cannot resolve or connect to the target even though pods "
            "may be healthy and listening. Users observe request timeouts and cascading "
            "failures on flows that depend on this service endpoint."
        ),
    },
    "network_policy_block_dns": {
        "uses_rcode": False,
        "description_fn": lambda self: (
            f"A NetworkPolicy `{self._dns_policy_name}` in namespace {self.namespace} "
            f"denies all egress traffic from pods belonging to {self.faulty_service}. "
            "This blocks DNS resolution (port 53) as the first observable symptom, "
            "preventing the service from resolving any hostnames. CoreDNS itself is "
            "healthy and other pods resolve normally. The agent must inspect "
            "NetworkPolicies on the affected pods to find the deny-all-egress rule."
        ),
    },
}


class ServiceDNSResolutionFailure(Problem):
    def __init__(
        self,
        app_name="social_network",
        faulty_service="user-service",
        dns_response_code="NXDOMAIN",
        dns_failure_layer="coredns_template",
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.dns_response_code = dns_response_code
        self.dns_failure_layer = dns_failure_layer

        layer_config = _DNS_FAILURE_LAYERS[dns_failure_layer]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()

        # For network_policy_block_dns, build the policy name (needed before the
        # root cause description). The pod selector is derived from the live
        # Service's own selector at injection time (see inject_fault) rather than
        # a hardcoded per-app label key: the previous per-app map guessed wrong
        # keys for most apps (social_network pods carry `service:`/`app:`,
        # astronomy_shop pods carry `app.kubernetes.io/name:`), so the policy
        # matched zero pods and DNS was never blocked (a silent no-op fault).
        if self.dns_failure_layer == "network_policy_block_dns":
            self._dns_policy_name = f"deny-dns-egress-{faulty_service}"
            self._networking_v1 = client.NetworkingV1Api()

        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=layer_config["description_fn"](self),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = DNSResolutionMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        if self.dns_failure_layer == "coredns_template":
            self.injector = VirtualizationFaultInjector(namespace=self.namespace)
            self.injector._inject(
                fault_type="service_dns_resolution_failure",
                microservices=[self.faulty_service],
                rcode=self.dns_response_code,
            )
        elif self.dns_failure_layer == "network_policy_block_dns":
            # Deny-all-egress NetworkPolicy on the faulty service's pods.
            #
            # Kubernetes NetworkPolicy egress semantics are whitelist-based:
            # when policyTypes includes "Egress", ALL egress is denied by
            # default and only traffic matching the `egress` rules is allowed.
            # An empty egress list (egress: []) therefore blocks ALL outbound
            # traffic, including DNS on port 53.
            #
            # This is intentionally a deny-all policy rather than a
            # "block-only-port-53" policy because NetworkPolicy cannot express
            # "allow all ports except 53" in a single rule. DNS failure is the
            # first observable symptom since it precedes any TCP connection.
            #
            # Derive the pod selector from the Service's own selector so the
            # policy matches exactly the pods backing the faulty Service — the
            # same pods the DNSResolutionMitigationOracle targets. Reusing the
            # live selector is app-agnostic and avoids the earlier zero-match bug.
            svc = self.kubectl.get_service_json(self.faulty_service, self.namespace)
            pod_selector_labels = svc.get("spec", {}).get("selector")
            if not pod_selector_labels:
                raise ValueError(
                    f"Service '{self.faulty_service}' has no selector (headless/ExternalName?); "
                    "cannot target its pods with a NetworkPolicy."
                )
            policy = client.V1NetworkPolicy(
                api_version="networking.k8s.io/v1",
                kind="NetworkPolicy",
                metadata=client.V1ObjectMeta(
                    name=self._dns_policy_name,
                    namespace=self.namespace,
                ),
                spec=client.V1NetworkPolicySpec(
                    pod_selector=client.V1LabelSelector(
                        match_labels=pod_selector_labels,
                    ),
                    policy_types=["Egress"],
                    egress=[],  # empty list = deny all egress
                ),
            )
            self._networking_v1.create_namespaced_network_policy(
                namespace=self.namespace,
                body=policy,
            )
        print(f"Service: {self.faulty_service} | Layer: {self.dns_failure_layer} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        if self.dns_failure_layer == "coredns_template":
            self.injector = VirtualizationFaultInjector(namespace=self.namespace)
            self.injector._recover(
                fault_type="service_dns_resolution_failure",
                microservices=[self.faulty_service],
            )
        elif self.dns_failure_layer == "network_policy_block_dns":
            try:
                self._networking_v1.delete_namespaced_network_policy(
                    name=self._dns_policy_name,
                    namespace=self.namespace,
                )
            except ApiException as error:
                if error.status != 404:
                    raise
        print(f"Service: {self.faulty_service} | Layer: {self.dns_failure_layer} | Namespace: {self.namespace}\n")
