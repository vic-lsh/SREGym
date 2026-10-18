import json

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.workload import WorkloadOracle
from sregym.conductor.problems.base import Problem
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

SYSCTL_NAME = "net.ipv4.ip_local_port_range"
DEFAULT_RANGE = "32768 60999"

_PORT_RANGE_SEVERITIES = {
    "critical": {
        "range": "32000 32003",
        "description": (
            "narrows the ephemeral port range to 4 ports. The frontend "
            "cannot establish outbound TCP connections to all gRPC backends; "
            "at least one backend is always unreachable with EADDRNOTAVAIL "
            "even though pods remain Running."
        ),
    },
    "degraded": {
        "range": "32000 32015",
        "description": (
            "narrows the ephemeral port range to 16 ports, causing "
            "intermittent connection failures under load as ephemeral ports "
            "are exhausted when multiple gRPC backends are called concurrently."
        ),
    },
}

_SYSCTL_SCOPES = {
    "all_deployments": {
        "component": "all hotel-reservation deployments",
        "faulty_service": "all",
        "subject": "All hotel-reservation pods",
    },
    "frontend_only": {
        "component": "hotel-reservation frontend deployment",
        "faulty_service": "frontend",
        "subject": "The frontend pod",
    },
}


class EphemeralPortRangeHotelReservation(Problem):
    def __init__(
        self,
        port_range_severity: str = "critical",
        sysctl_scope: str = "all_deployments",
    ):
        super().__init__(app=HotelReservation())
        self.kubectl = KubeCtl()

        severity_config = _PORT_RANGE_SEVERITIES[port_range_severity]
        self.bad_range = severity_config["range"]

        scope_config = _SYSCTL_SCOPES[sysctl_scope]
        self.sysctl_scope = sysctl_scope
        self.faulty_service = scope_config["faulty_service"]

        self.root_cause = self.build_structured_root_cause(
            component=scope_config["component"],
            namespace=self.namespace,
            description=(
                f"{scope_config['subject']} set {SYSCTL_NAME} to "
                f"'{self.bad_range}', which {severity_config['description']}"
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        # Workload checks catch failures even when pods stay Running.
        self.app.create_workload()
        self.mitigation_oracle = WorkloadOracle(problem=self, wrk_manager=self.app.wrk)

    # These are not microservice peers — patching them causes Consul to lose
    # service registrations on restart, breaking discovery for app services.
    _SYSCTL_EXCLUDE = frozenset({"consul", "jaeger"})

    def _filter_deployments(self, deployments):
        """Filter deployments based on the sysctl_scope."""
        if self.sysctl_scope == "frontend_only":
            return [d for d in deployments if d.metadata.name == "frontend"]
        return deployments

    def _patch_sysctl(self, value: str | None):
        deployments = [
            d
            for d in self.kubectl.list_deployments(self.namespace).items
            if d.metadata.name not in self._SYSCTL_EXCLUDE
        ]
        deployments = self._filter_deployments(deployments)
        if not deployments:
            raise RuntimeError("No deployments found to patch sysctl.")

        if value is None:
            patch = {
                "spec": {
                    "template": {
                        "spec": {
                            "securityContext": {
                                "sysctls": None,
                            }
                        }
                    }
                }
            }
        else:
            patch = {
                "spec": {
                    "template": {
                        "spec": {
                            "securityContext": {
                                "sysctls": [
                                    {
                                        "name": SYSCTL_NAME,
                                        "value": value,
                                    }
                                ]
                            }
                        }
                    }
                }
            }

        payload = json.dumps(patch)
        for dep in deployments:
            name = dep.metadata.name
            self.kubectl.exec_command(
                f"kubectl -n {self.namespace} patch deployment {name} --type=merge -p '{payload}'"
            )

    def _wait_for_rollouts(self):
        deployments = self.kubectl.list_deployments(self.namespace).items
        deployments = self._filter_deployments(deployments)
        for dep in deployments:
            name = dep.metadata.name
            self.kubectl.exec_command(f"kubectl -n {self.namespace} rollout status deployment/{name} --timeout=300s")

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self._patch_sysctl(self.bad_range)
        self._wait_for_rollouts()

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        # Restore the default range explicitly for deterministic recovery.
        self._patch_sysctl(DEFAULT_RANGE)
        self._wait_for_rollouts()
