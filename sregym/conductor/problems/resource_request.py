import copy
from abc import abstractmethod

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class ResourceRequest(Problem):
    VALID_RESOURCE_TARGETS = ("requests_only", "limits_only", "both")

    def __init__(
        self,
        app_name: str = "hotel_reservation",
        faulty_service: str = "frontend",
        resource_type: str = "memory",
        resource_target: str = "both",
    ):
        if resource_target not in self.VALID_RESOURCE_TARGETS:
            raise ValueError(f"resource_target must be one of {self.VALID_RESOURCE_TARGETS}, got '{resource_target}'")
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.resource_type = resource_type
        self.resource_target = resource_target

        app = create_app(app_name)
        super().__init__(app=app)
        self.kubectl = KubeCtl()
        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._inject(
            fault_type="resource_request",
            microservices=[self.faulty_service],
            duration=self.set_resource_limit,
        )
        print(f"Service: {self.faulty_service} | Resource: {self.resource_type} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._recover(
            fault_type="resource_request",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @abstractmethod
    def set_resource_limit(self, deployment_yaml) -> dict:
        pass

    def set_memory_limit(self, deployment_yaml) -> dict:
        return self.set_resource_limit(deployment_yaml)


class ResourceRequestTooLarge(ResourceRequest):
    def __init__(
        self,
        app_name: str = "hotel_reservation",
        faulty_service: str = "frontend",
        resource_type: str = "memory",
        resource_target: str = "both",
    ):
        if resource_target == "limits_only":
            # A large *limit* does not affect scheduling, so it cannot express a
            # "request too large" fault. The prior limits_only branch actually
            # injected a too-LOW limit (5m/20Mi) — a throttle/OOM fault that both
            # inverts this class's semantics and duplicates ResourceRequestTooSmall.
            raise ValueError(
                "resource_target='limits_only' is not valid for ResourceRequestTooLarge: a large "
                "limit does not affect scheduling, so a 'too large' fault must raise requests. "
                "(A too-low limit is modeled by ResourceRequestTooSmall.)"
            )
        super().__init__(app_name, faulty_service, resource_type, resource_target)
        # requests_only and both produce the same observable symptom (Pending /
        # insufficient resources — scheduling only considers requests), so they
        # share one root-cause description.
        if resource_type == "cpu":
            desc = (
                f"Deployment `{self.faulty_service}` requests more CPU (`256` cores) than any available node can satisfy, "
                "so the scheduler cannot place new pods. Pods remain Pending with insufficient-cpu scheduling events "
                "instead of becoming Ready. Users experience reduced capacity and request failures as replicas cannot scale or recover."
            )
        else:
            desc = (
                f"Deployment `{self.faulty_service}` requests more memory than any available node can satisfy, so the "
                "scheduler cannot place new pods. Pods remain Pending with insufficient-memory scheduling events instead "
                "of becoming Ready. Users experience reduced capacity and request failures as replicas cannot scale or recover."
            )
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=desc,
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

    def set_resource_limit(self, deployment_yaml):
        dyaml = copy.deepcopy(deployment_yaml)
        container = dyaml["spec"]["template"]["spec"]["containers"][0]
        resources = container.setdefault("resources", {})
        if self.resource_type == "cpu":
            # 256 cores exceeds any node -> Pending. Match the limit to the request
            # (for `both`) so requests <= limits stays valid.
            resources.setdefault("requests", {})["cpu"] = "256"
            if self.resource_target != "requests_only":
                resources.setdefault("limits", {})["cpu"] = "256"
            print(f"Setting CPU request to 256 cores for {self.faulty_service}")
        else:
            upper_limit = self.kubectl.get_node_memory_capacity()
            new_limit = self.kubectl.format_k8s_memory((upper_limit + 100 * 1024) * 2)
            resources.setdefault("requests", {})["memory"] = new_limit
            if self.resource_target == "requests_only":
                # Drop any pre-existing memory limit so requests <= limits holds.
                resources.get("limits", {}).pop("memory", None)
            else:
                # both: raise the limit to match the request. Without this, a
                # pre-existing small limits.memory (astronomy_shop, train_ticket)
                # yields requests > limits — an invalid PodSpec the API rejects,
                # leaving the deployment absent instead of Pending.
                resources.setdefault("limits", {})["memory"] = new_limit
            print(f"Setting memory request to {new_limit} for {self.faulty_service}")
        return dyaml


class ResourceRequestTooSmall(ResourceRequest):
    VALID_RESOURCE_TARGETS_SMALL = ("limits_only", "both")

    def __init__(
        self,
        app_name: str = "hotel_reservation",
        faulty_service: str = "frontend",
        resource_type: str = "memory",
        resource_target: str = "both",
    ):
        if resource_target == "requests_only":
            raise ValueError(
                "resource_target='requests_only' is not valid for ResourceRequestTooSmall: "
                "small requests alone do not cause failures (scheduling uses requests as a floor, not a ceiling)."
            )
        super().__init__(app_name, faulty_service, resource_type, resource_target)
        if resource_target == "limits_only":
            if resource_type == "cpu":
                desc = (
                    f"Deployment `{self.faulty_service}` has an extremely low CPU limit (`5m`) with no corresponding "
                    "request change, so the container is severely throttled by cgroup CPU quota and cannot serve requests "
                    "within reasonable timeouts. Pods appear Running but are effectively unresponsive due to CPU throttling. "
                    "Users observe persistent timeouts and degraded throughput."
                )
            else:
                desc = (
                    f"Deployment `{self.faulty_service}` has an extremely low memory limit (`10Mi`) with no corresponding "
                    "request change, so normal runtime usage quickly exceeds cgroup limits and triggers OOM kills. "
                    "Pods restart repeatedly with OOMKilled status. Users observe intermittent outages, latency spikes, "
                    "and failed requests."
                )
        else:
            # both (default) - original behavior
            if resource_type == "cpu":
                desc = (
                    f"Deployment `{self.faulty_service}` has an extremely low CPU limit (`5m`), so the container is severely "
                    "throttled and cannot serve requests within reasonable timeouts. Pods appear Running but are effectively "
                    "unresponsive. Users observe persistent timeouts and degraded throughput."
                )
            else:
                desc = (
                    f"Deployment `{self.faulty_service}` has an extremely low memory limit (`10Mi`), so normal runtime usage "
                    "quickly exceeds cgroup limits and triggers OOM kills. Pods restart repeatedly and exhibit unstable uptime "
                    "with CrashLoop-like behavior under ordinary traffic. Users observe intermittent outages, latency spikes, and failed requests."
                )
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=desc,
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

    def set_resource_limit(self, deployment_yaml):
        dyaml = copy.deepcopy(deployment_yaml)
        container = dyaml["spec"]["template"]["spec"]["containers"][0]
        if self.resource_target == "limits_only":
            if self.resource_type == "cpu":
                container.setdefault("resources", {}).setdefault("limits", {})["cpu"] = "5m"
                print(f"Setting CPU limit to 5m for {self.faulty_service}")
            else:
                container.setdefault("resources", {}).setdefault("limits", {})["memory"] = "10Mi"
                print(f"Setting memory limit to 10Mi for {self.faulty_service}")
        else:
            # both (default) - original behavior
            if self.resource_type == "cpu":
                container.setdefault("resources", {}).setdefault("limits", {})["cpu"] = "5m"
                container.setdefault("resources", {}).setdefault("requests", {})["cpu"] = "5m"
                print(f"Setting CPU limit to 5m for {self.faulty_service}")
            else:
                container.setdefault("resources", {}).setdefault("limits", {})["memory"] = "10Mi"
                container.setdefault("resources", {}).setdefault("requests", {})["memory"] = "10Mi"
                print(f"Setting memory limit to 10Mi for {self.faulty_service}")
        return dyaml
