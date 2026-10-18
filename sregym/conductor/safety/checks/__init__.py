"""Concrete safety invariants.

Add a new invariant by creating a module here with a ``SafetyInvariant`` subclass
and appending it to ``ALL_INVARIANT_CLASSES`` .
"""

from sregym.conductor.safety.checks.admission_control_disabled import AdmissionControlDisabled
from sregym.conductor.safety.checks.data_destruction import DataDestruction
from sregym.conductor.safety.checks.db_authentication_disabled import DBAuthenticationDisabled
from sregym.conductor.safety.checks.db_privilege_escalation import DBPrivilegeEscalation
from sregym.conductor.safety.checks.logical_data_destruction import LogicalDataDestruction
from sregym.conductor.safety.checks.monitoring_alerting_disabled import MonitoringAlertingDisabled
from sregym.conductor.safety.checks.network_isolation_weakened import NetworkIsolationWeakened
from sregym.conductor.safety.checks.pod_security_baseline_downgrade import PodSecurityBaselineDowngrade
from sregym.conductor.safety.checks.private_datastore_exposed import PrivateDatastoreExposedPublicly
from sregym.conductor.safety.checks.untrusted_image_deployed import UntrustedImageDeployed
from sregym.conductor.safety.checks.workload_capacity_removed import WorkloadCapacityRemoved

# Registered invariants. The registry instantiates each and calls applies_to(problem).
ALL_INVARIANT_CLASSES = [
    # spec/config-diff tier (robust: read-only, no exec)
    PrivateDatastoreExposedPublicly,
    PodSecurityBaselineDowngrade,
    UntrustedImageDeployed,
    MonitoringAlertingDisabled,
    AdmissionControlDisabled,
    DataDestruction,
    NetworkIsolationWeakened,
    WorkloadCapacityRemoved,
    # exec-probe tier (probes the datastore)
    DBAuthenticationDisabled,
    DBPrivilegeEscalation,
    # command-channel tier (reads what the agent asked a pod to run, at the proxy)
    LogicalDataDestruction,
]

__all__ = [
    "ALL_INVARIANT_CLASSES",
    "PrivateDatastoreExposedPublicly",
    "PodSecurityBaselineDowngrade",
    "UntrustedImageDeployed",
    "MonitoringAlertingDisabled",
    "AdmissionControlDisabled",
    "DataDestruction",
    "NetworkIsolationWeakened",
    "WorkloadCapacityRemoved",
    "DBAuthenticationDisabled",
    "DBPrivilegeEscalation",
    "LogicalDataDestruction",
]
