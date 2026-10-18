from sregym.conductor.oracles.dns_resolution_mitigation import DNSResolutionMitigationOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_DNS_MISCONFIG_PROFILES = {
    "external_resolver": {
        "inject_patch": (
            '[{"op":"replace","path":"/spec/template/spec/dnsPolicy","value":"None"},'
            '{"op":"add","path":"/spec/template/spec/dnsConfig","value":'
            '{"nameservers":["8.8.8.8"],"searches":[]}}]'
        ),
        "recover_patch": (
            '[{"op":"remove","path":"/spec/template/spec/dnsPolicy"},'
            '{"op":"remove","path":"/spec/template/spec/dnsConfig"}]'
        ),
        "propagation_ns": "8.8.8.8",
        "inject_expect": True,
        "recover_expect": False,
        "description": (
            "Deployment `{faulty_service}` is configured with an invalid DNS policy (`dnsPolicy: None`) and "
            "external resolver (`8.8.8.8`), so it cannot reliably resolve cluster-internal service names. Calls "
            "to `*.svc.cluster.local` dependencies fail with name resolution errors even when target services are "
            "healthy. Users see request timeouts and cascading failures on dependent application paths."
        ),
    },
    "default_policy": {
        "inject_patch": ('[{"op":"replace","path":"/spec/template/spec/dnsPolicy","value":"Default"}]'),
        "recover_patch": ('[{"op":"replace","path":"/spec/template/spec/dnsPolicy","value":"ClusterFirst"}]'),
        "propagation_ns": "cluster.local",
        "inject_expect": False,
        "recover_expect": True,
        "description": (
            "Deployment `{faulty_service}` is configured with `dnsPolicy: Default` which bypasses cluster DNS "
            "(kube-dns/CoreDNS) and uses the node's resolv.conf instead. Short-name cluster-internal lookups "
            "fail because the node resolv.conf lacks the `.svc.cluster.local` search domain. Calls to "
            "cluster-internal services fail with name resolution errors, causing request timeouts and cascading "
            "failures on dependent application paths."
        ),
    },
}


class WrongDNSPolicy(Problem):
    def __init__(
        self, app_name="social_network", faulty_service="user-service", dns_misconfiguration_type="external_resolver"
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.dns_misconfiguration_type = dns_misconfiguration_type
        self._profile = _DNS_MISCONFIG_PROFILES[dns_misconfiguration_type]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=self._profile["description"].format(faulty_service=self.faulty_service),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = DNSResolutionMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector.inject_wrong_dns_policy(
            microservices=[self.faulty_service],
            dns_patch=self._profile["inject_patch"],
            propagation_ns=self._profile["propagation_ns"],
            expect_external=self._profile["inject_expect"],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector.recover_wrong_dns_policy(
            microservices=[self.faulty_service],
            recovery_patch=self._profile["recover_patch"],
            propagation_ns=self._profile["propagation_ns"],
            expect_external=self._profile["recover_expect"],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
