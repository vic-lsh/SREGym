"""Benchmark-only injection, component and cleanup contracts; no agent-facing state."""

from __future__ import annotations

import inspect
import math
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

PROTOCOL_VERSION = 2


@dataclass(frozen=True)
class InjectionResult:
    valid: bool
    infra_error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_failure(cls, failure: dict[str, Any] | None) -> InjectionResult:
        return cls(failure is None, None if failure is None else "injection_invalid", failure or {})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FaultComponent:
    component_index: int
    injection_order: int
    identity: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecoveryFailure:
    component_index: int | None
    status: str
    error: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RecoveryResult:
    recovered: list[int] = field(default_factory=list)
    pending: list[int] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        status = "recovered"
        if self.errors:
            status = (
                "partial"
                if self.recovered
                else ("timeout" if any(e["status"] == "timeout" for e in self.errors) else "error")
            )
        return {"success": not self.errors, "status": status, **asdict(self)}


def run_recovery(call: Callable[[], Any], timeout_s: float) -> tuple[str, str]:
    """Recover under a deadline, bypassing legacy decorators that swallow exceptions.

    A timeout cannot stop Python code. Owners must retain the timed-out component and prevent
    reuse of its warm lane, even if the daemon eventually exits.
    """
    if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
        raise TypeError("recovery timeout must be a number")
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("recovery timeout must be finite and positive")
    outcome: list[tuple[str, str]] = []

    def target() -> None:
        try:
            raw = inspect.unwrap(call)
            if raw is not call and inspect.ismethod(call):
                raw(call.__self__)
            else:
                raw()
            outcome.append(("recovered", ""))
        except BaseException as error:  # cleanup must also report a legacy injector's SystemExit
            outcome.append(("error", f"{type(error).__name__}: {error}"))

    thread = threading.Thread(target=target, daemon=True, name="fault-recovery")
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        return "timeout", f"timeout after {timeout_s:g}s; warm lane cannot be reused"
    return outcome[0]
