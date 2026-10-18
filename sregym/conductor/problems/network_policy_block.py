import hashlib

from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.network_policy_oracle import NetworkPolicyMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.paths import TARGET_MICROSERVICES
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_POLICY_NAME_STYLES = {
    "descriptive": {
        "name_fn": lambda direction_label, faulty_service: f"deny-{direction_label}-{faulty_service}",
        "description": "The policy has a descriptive name that clearly indicates its purpose.",
    },
    "opaque": {
        "name_fn": lambda direction_label, faulty_service: (
            f"np-ext-{hashlib.md5(faulty_service.encode()).hexdigest()[:6]}"
        ),
        "description": "The policy has an opaque, hash-based name that does not reveal its purpose, requiring inspection of the policy spec to understand its effect.",
    },
    "misleading": {
        "name_fn": lambda direction_label, faulty_service: f"allow-traffic-{faulty_service}",
        "description": "The policy has a misleading name suggesting it allows traffic, when it actually blocks all traffic to the target service.",
    },
}


class NetworkPolicyBlock(Problem):
    POD_LABEL_KEY = "io.kompose.service"

    def __init__(
        self,
        app_name: str = "hotel_reservation",
        faulty_service: str = "recommendation",
        policy_direction: str = "both",
        policy_name_style: str = "descriptive",
    ):
        app = create_app(app_name)
        super().__init__(app=app)
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.policy_direction = policy_direction
        self.policy_name_style = policy_name_style
        self.kubectl = KubeCtl()
        if app_name in ["astronomy_shop", "train_ticket"]:
            self.pod_label_key = "app"
        elif app_name == "social_network":
            self.pod_label_key = "app.kubernetes.io/name"
        else:
            self.pod_label_key = "io.kompose.service"
        direction_label = {"both": "all", "ingress_only": "ingress-only", "egress_only": "egress-only"}.get(
            policy_direction, policy_direction
        )
        style_config = _POLICY_NAME_STYLES[policy_name_style]
        self.policy_name = style_config["name_fn"](direction_label, faulty_service)

        self.app.payload_script = (
            TARGET_MICROSERVICES / "hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        )
        self.app.create_workload()

        if policy_direction == "ingress_only":
            direction_desc = "ingress"
            traffic_desc = "incoming"
        elif policy_direction == "egress_only":
            direction_desc = "egress"
            traffic_desc = "outgoing"
        else:
            direction_desc = "ingress and egress"
            traffic_desc = "incoming and outgoing"

        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"A NetworkPolicy `{self.policy_name}` blocks all {direction_desc} traffic for pods labeled "
                f"`{self.pod_label_key}={self.faulty_service}`, preventing all {traffic_desc} network "
                "communication for the target workload. Service-to-service communication fails, so dependent "
                "request paths break even if pods remain Running. Users observe hard failures/timeouts on flows "
                f"that require this service. {style_config['description']}"
            ),
        )
        self.networking_v1 = client.NetworkingV1Api()

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = NetworkPolicyMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        """Block traffic to/from the target service based on policy_direction."""
        spec = {
            "podSelector": {"matchLabels": {getattr(self, "pod_label_key", self.POD_LABEL_KEY): self.faulty_service}},
        }

        direction = getattr(self, "policy_direction", "both")
        if direction == "ingress_only":
            spec["policyTypes"] = ["Ingress"]
            spec["ingress"] = []
        elif direction == "egress_only":
            spec["policyTypes"] = ["Egress"]
            spec["egress"] = []
        else:
            spec["policyTypes"] = ["Ingress", "Egress"]
            spec["ingress"] = []
            spec["egress"] = []

        policy = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": self.policy_name, "namespace": self.namespace},
            "spec": spec,
        }
        try:
            self.networking_v1.create_namespaced_network_policy(namespace=self.namespace, body=policy)
        except ApiException as exc:
            if exc.status != 409:
                raise
            # A repaired source tree can ship a permissive policy of the same name; the fault replaces it.
            self.networking_v1.replace_namespaced_network_policy(
                name=self.policy_name, namespace=self.namespace, body=policy
            )

    @mark_fault_injected
    def recover_fault(self):
        """Remove the NetworkPolicy"""
        try:
            self.networking_v1.delete_namespaced_network_policy(name=self.policy_name, namespace=self.namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
