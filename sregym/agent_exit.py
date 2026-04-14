from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol

from sregym.agent_registry import AgentRegistration


DEFAULT_GRACEFUL_EXIT_TIMEOUT_SECONDS = 30.0


class PollableProcess(Protocol):
    returncode: int | None

    def poll(self) -> int | None: ...


def resolve_graceful_exit_timeout_seconds(agent_registration: AgentRegistration | None) -> float | None:
    """Return the graceful-exit timeout for the given agent.

    ``None`` means the runner should wait without a fixed timeout.
    """
    if agent_registration and agent_registration.wait_for_natural_exit:
        return None
    return DEFAULT_GRACEFUL_EXIT_TIMEOUT_SECONDS


async def wait_for_process_exit(
    proc: PollableProcess,
    *,
    timeout_seconds: float | None,
    poll_interval_seconds: float = 1.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> bool:
    """Wait for ``proc`` to exit.

    Returns ``True`` if the process exited naturally before the timeout, else
    ``False`` when ``timeout_seconds`` is exceeded.
    """
    elapsed = 0.0
    while True:
        proc.poll()
        if proc.returncode is not None:
            return True
        if timeout_seconds is not None and elapsed >= timeout_seconds:
            return False
        await sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds
