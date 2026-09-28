"""JSON-lines worker for a fast inner development loop (opt-in; nothing else imports it).

A driver outside SREGym runs many incidents against one warm deployment:

    cluster  -> create or reuse the kind cluster (``sregym.worker_infra``)
    deploy   -> shared infrastructure + application + workload, skipped while the app is healthy
    inject   -> a fresh problem instance injects its fault; with ``compose`` it joins the faults
                already injected, so several problems can be live at once (a composite fault)
    oracle   -> that problem's mitigation oracle (deterministic), or a health check if it has none;
                a composition passes only when every fault's oracle passes (``fault`` grades one)
    recover  -> that problem recovers its fault; a composition recovers every fault, the last
                injected first, or only the one ``fault`` names

It reuses the problem classes, fault injectors, oracles, and ``Conductor.deploy_app`` for the
one-time deployment, but never the conductor's stage machine, HTTP API, LLM judge, undeploy, or
cluster reconciliation. Requests and replies are one JSON object per line; everything the
harness prints goes to stderr so it cannot corrupt the protocol.

Run: ``python -m sregym.fastloop.worker`` from the SREGym root, with ``KUBECONFIG`` (or a
``cluster`` request first) and the usual ``SREGYM_*`` deployment environment.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from typing import IO, Any, Protocol

logger = logging.getLogger("all.sregym.fastloop")


class _Oracle(Protocol):
    def evaluate(self) -> dict: ...


class _Problem(Protocol):
    app: Any
    mitigation_oracle: _Oracle | None

    def inject_fault(self) -> None: ...

    def recover_fault(self) -> None: ...


ProblemFactory = Callable[[str], _Problem]
Deployer = Callable[[_Problem, str], None]
HealthProbe = Callable[[str], dict]
ClusterFactory = Callable[[int, str], tuple[str, str]]


class _Proxy(Protocol):
    def start(self) -> None: ...

    def generate_agent_kubeconfig(self, output_path: str | None = None) -> str: ...


ProxyFactory = Callable[[set[str], int], _Proxy]
PromptBuilder = Callable[[dict[str, Any], str], str]
#: Raises unless the agent kubeconfig at ``path`` reaches only this proxy ``port`` and this cluster.
KubeconfigVerifier = Callable[[str, int], None]
#: Namespaces the conductor's agent proxy always hides (``Conductor.__init__``).
BENCHMARK_HIDDEN_NAMESPACES = frozenset({"chaos-mesh", "khaos"})


class FastloopWorker:
    """Serve one request at a time; holds the problem instance of the incident in flight."""

    def __init__(
        self,
        *,
        problem_factory: ProblemFactory,
        deployer: Deployer,
        health_probe: HealthProbe,
        cluster_factory: ClusterFactory,
        proxy_factory: ProxyFactory | None = None,
        prompt_builder: PromptBuilder | None = None,
        kubeconfig_verifier: KubeconfigVerifier | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._problem_factory = problem_factory
        self._deployer = deployer
        self._health_probe = health_probe
        self._cluster_factory = cluster_factory
        self._proxy_factory = proxy_factory
        self._prompt_builder = prompt_builder
        self._kubeconfig_verifier = kubeconfig_verifier
        self._clock = clock
        #: The faults injected since the last plain ``inject``; ``compose`` injections join them.
        self._incidents: list[_Problem] = []
        self._recovered: set[int] = set()
        self._proxy: dict[str, Any] | None = None

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        op = request.get("op")
        handler = {
            "cluster": self._cluster,
            "deploy": self._deploy,
            "app_info": self._app_info,
            "health": self._health,
            "inject": self._inject,
            "oracle": self._oracle,
            "recover": self._recover,
            "proxy": self._start_proxy,
            "codex_prompt": self._codex_prompt,
            "shutdown": lambda _request: {},
        }.get(op if isinstance(op, str) else "")
        if handler is None:
            raise ValueError(f"unknown op {op!r}")
        return handler(request)

    def _cluster(self, request: dict[str, Any]) -> dict[str, Any]:
        name, kubeconfig = self._cluster_factory(int(request["worker_id"]), str(request["log_dir"]))
        os.environ["KUBECONFIG"] = kubeconfig
        return {"cluster": name, "kubeconfig": kubeconfig}

    def _deploy(self, request: dict[str, Any]) -> dict[str, Any]:
        problem = self._problem_factory(str(request["problem_id"]))
        health = self._health_probe(problem.app.namespace)
        if health.get("healthy") and not request.get("redeploy"):
            return {"deployed": False, "seconds": 0.0, "health": health}
        started = self._clock()
        self._deployer(problem, str(request["baseline_path"]))
        return {
            "deployed": True,
            "seconds": self._clock() - started,
            "health": self._health_probe(problem.app.namespace),
        }

    def _app_info(self, request: dict[str, Any]) -> dict[str, Any]:
        app = self._problem_factory(str(request["problem_id"])).app
        return {"app_name": app.app_name, "namespace": app.namespace, "descriptions": str(app.description)}

    def _start_proxy(self, request: dict[str, Any]) -> dict[str, Any]:
        """Serve agents the conductor's filtered Kubernetes API on a port the caller chose."""

        if self._proxy is not None:
            return self._proxy
        if self._proxy_factory is None:
            raise RuntimeError("this worker has no agent proxy")
        hidden = set(BENCHMARK_HIDDEN_NAMESPACES) | {str(name) for name in request.get("hide", [])}
        port = int(request["port"])
        proxy = self._proxy_factory(hidden, port)
        proxy.start()
        started = {"kubeconfig": proxy.generate_agent_kubeconfig(str(request["kubeconfig"])), "port": port}
        # The conductor's crossover guard: never hand an agent a kubeconfig that reaches another cluster.
        self._verify_agent_kubeconfig(started)
        self._proxy = started
        return self._proxy

    def _verify_agent_kubeconfig(self, proxy: dict[str, Any]) -> None:
        if self._kubeconfig_verifier is not None:
            self._kubeconfig_verifier(str(proxy["kubeconfig"]), int(proxy["port"]))

    def _codex_prompt(self, request: dict[str, Any]) -> dict[str, Any]:
        if self._prompt_builder is None:
            raise RuntimeError("this worker has no Codex prompt builder")
        return {"prompt": self._prompt_builder(self._app_info(request), str(request["api_base"]))}

    def _health(self, request: dict[str, Any]) -> dict[str, Any]:
        return self._health_probe(str(request["namespace"]))

    def _inject(self, request: dict[str, Any]) -> dict[str, Any]:
        """Inject one fault; with ``compose`` it joins the faults injected since the last plain inject.

        The reply's ``fault`` is its index in the composition, for ``recover`` and ``oracle``.
        """

        # A fresh instance per incident, as SREGym constructs one per problem run; its oracle
        # snapshots the healthy deployment before the fault.
        problem = self._problem_factory(str(request["problem_id"]))
        if self._proxy is not None:
            # As the conductor does right before ``inject_fault``.
            self._verify_agent_kubeconfig(self._proxy)
        if not request.get("compose"):
            self._incidents, self._recovered = [], set()
        self._incidents.append(problem)
        started = self._clock()
        problem.inject_fault()
        return {"started_at": started, "finished_at": self._clock(), "fault": len(self._incidents) - 1}

    @property
    def _incident(self) -> _Problem | None:
        return self._incidents[-1] if self._incidents else None

    def _fault(self, request: dict[str, Any]) -> int | None:
        raw = request.get("fault")
        if raw is None:
            return None
        index = int(raw)
        if not 0 <= index < len(self._incidents):
            raise ValueError(f"no fault {index}; {len(self._incidents)} injected")
        return index

    def _current(self) -> _Problem:
        if self._incident is None:
            raise RuntimeError("no injected incident; send an inject request first")
        return self._incident

    def _grade(self, problem: _Problem) -> dict[str, Any]:
        if problem.mitigation_oracle is None:
            health = self._health_probe(problem.app.namespace)
            return {"kind": "health-check", "success": bool(health.get("healthy")), "details": health}
        details = problem.mitigation_oracle.evaluate()
        return {"kind": "sregym-mitigation-oracle", "success": bool(details.get("success")), "details": details}

    def _oracle(self, request: dict[str, Any]) -> dict[str, Any]:
        self._current()
        index = self._fault(request)
        if index is not None:
            return self._grade(self._incidents[index])
        if len(self._incidents) == 1:
            return self._grade(self._incidents[0])
        grades = [self._grade(problem) for problem in self._incidents]
        return {
            "kind": "composition",
            "success": all(grade["success"] for grade in grades),
            "details": {"faults": grades},
        }

    def _recover(self, request: dict[str, Any]) -> dict[str, Any]:
        """Recover one fault (``fault``) or every unrecovered fault, the last injected first."""

        self._current()
        index = self._fault(request)
        if index is not None and index in self._recovered:
            raise ValueError(f"fault {index} is already recovered")
        pending = (
            [index]
            if index is not None
            else [i for i in reversed(range(len(self._incidents))) if i not in self._recovered]
        )
        if not pending:
            raise RuntimeError("no injected incident is left to recover; send an inject request first")
        started = self._clock()
        for fault in pending:
            self._incidents[fault].recover_fault()
            self._recovered.add(fault)
        return {"seconds": self._clock() - started, "recovered": pending}


def serve(worker: FastloopWorker, requests: IO[str], replies: IO[str]) -> None:
    """Answer each JSON request line with one JSON reply line until ``shutdown`` or EOF."""

    for line in requests:
        if not line.strip():
            continue
        request_id = None
        op = None
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            request_id = request.get("id")
            op = request.get("op")
            reply: dict[str, Any] = {"id": request_id, "ok": True, "result": worker.handle(request)}
        except Exception as exc:  # reported to the driver; the worker keeps serving
            logger.exception("fastloop request failed")
            reply = {"id": request_id, "ok": False, "error": f"{type(exc).__name__}: {exc}"}
        replies.write(json.dumps(reply, default=str) + "\n")
        replies.flush()
        if op == "shutdown" and reply["ok"]:
            return


def _health_probe(namespace: str) -> dict[str, Any]:
    from sregym.service.kubectl import KubeCtl

    kubectl = KubeCtl()
    try:
        deployments = kubectl.list_deployments(namespace).items
    except Exception as exc:
        return {"healthy": False, "namespace": namespace, "unready": [], "error": str(exc)}
    unready = sorted(
        deployment.metadata.name
        for deployment in deployments
        if (deployment.status.ready_replicas or 0)
        < (deployment.spec.replicas if deployment.spec.replicas is not None else 1)
    )
    return {"healthy": bool(deployments) and not unready, "namespace": namespace, "unready": unready}


def _deploy(problem: _Problem, baseline_path: str) -> None:
    from pathlib import Path

    import sregym.conductor.conductor as conductor_module

    # The persisted baseline is this run's own file, never a host-wide or per-cluster default
    # another run on the same cluster name could share.
    conductor_module.cluster_baseline_state_file = lambda: Path(baseline_path)
    conductor = conductor_module.Conductor(
        conductor_module.ConductorConfig(deploy_loki=False, preserve_infrastructure=True)
    )
    # The MCP server's host port-forward uses a fixed port and kills its previous owner.
    conductor.mcp_server.deploy = lambda: None
    conductor.problem = problem
    conductor.app = problem.app
    conductor.deploy_app()


def production_worker() -> FastloopWorker:
    registry: list[Any] = []

    def problem_factory(problem_id: str) -> _Problem:
        if not registry:
            from sregym.conductor.problems.registry import ProblemRegistry

            registry.append(ProblemRegistry())
        return registry[0].get_problem_instance(problem_id)

    def cluster_factory(worker_id: int, log_dir: str) -> tuple[str, str]:
        from sregym.worker_infra import create_worker_cluster

        return create_worker_cluster(worker_id, log_dir)

    def proxy_factory(hidden_namespaces: set[str], listen_port: int) -> _Proxy:
        from sregym.service.k8s_proxy import KubernetesAPIProxy

        return KubernetesAPIProxy(hidden_namespaces=hidden_namespaces, listen_port=listen_port)

    def kubeconfig_verifier(path: str, listen_port: int) -> None:
        from sregym.service.k8s_proxy import verify_agent_kubeconfig

        verify_agent_kubeconfig(
            path,
            listen_port=listen_port,
            cluster_name=os.environ.get("SREGYM_KIND_CLUSTER_NAME", "").strip() or None,
        )

    return FastloopWorker(
        problem_factory=problem_factory,
        deployer=_deploy,
        health_probe=_health_probe,
        cluster_factory=cluster_factory,
        proxy_factory=proxy_factory,
        prompt_builder=_codex_prompt,
        kubeconfig_verifier=kubeconfig_verifier,
    )


@contextlib.contextmanager
def cluster_lock_from_environment() -> Iterator[None]:
    """Hold the host-wide lock on ``SREGYM_KIND_CLUSTER_NAME`` for the worker's lifetime.

    The same lock the parallel runner holds per cluster, so a fast loop and a benchmark
    experiment (or two fast loops) never drive one cluster at once.
    """

    cluster = os.environ.get("SREGYM_KIND_CLUSTER_NAME", "").strip()
    if not cluster:
        yield
        return
    from sregym.worker_infra import cluster_lock

    with cluster_lock(cluster):
        yield


def _codex_prompt(app_info: dict[str, Any], api_base: str) -> str:
    """The raw Codex baseline's instruction, byte for byte, pointed at ``api_base``."""

    from urllib.parse import urlsplit

    from clients.codex.driver import build_instruction

    endpoint = urlsplit(api_base)
    previous = {name: os.environ.get(name) for name in ("API_HOSTNAME", "API_PORT")}
    os.environ["API_HOSTNAME"] = endpoint.hostname or "127.0.0.1"
    os.environ["API_PORT"] = str(endpoint.port or 80)
    try:
        return build_instruction(app_info)
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def main() -> int:
    # Keep the protocol on the original stdout; the harness prints freely to stderr.
    replies = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with cluster_lock_from_environment():
        serve(production_worker(), sys.stdin, replies)
    return 0


if __name__ == "__main__":
    sys.exit(main())
