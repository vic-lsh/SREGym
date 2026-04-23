"""Problem base class"""

from abc import ABC, abstractmethod
from typing import ClassVar

from sregym.service.apps.app_names import AppName, canonical_app_names


class Problem(ABC):
    TARGET_APPS: ClassVar[frozenset[str | AppName]] = frozenset()

    def __init__(self, app, namespace: str):
        self.app = app
        self.namespace = namespace
        self.fault_injected = False
        self.results = {}
        self.root_cause = None  # root cause of the problem in natural language

        # Optional: attach oracles in subclass
        self.diagnosis_oracle = None
        self.mitigation_oracle = None

    def requires_khaos(self) -> bool:
        """Override this method to return True if the problem requires Khaos for fault injection."""
        return False

    @abstractmethod
    def inject_fault(self):
        pass

    @abstractmethod
    def recover_fault(self):
        pass

    def verify_fault_applied(self) -> None:
        """Optionally assert that the fault is actually in effect after inject_fault.

        Default: no-op. Subclasses override to query live cluster state and raise
        (any exception) if the expected faulty post-state isn't present. The deploy
        flow calls this right after inject_fault; a raise fails the deploy before
        the environment is handed out.
        """
        return None

    @classmethod
    def target_apps(cls) -> frozenset[str]:
        return canonical_app_names(cls.TARGET_APPS)
