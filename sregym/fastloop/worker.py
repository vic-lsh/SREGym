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

A registry composite (``composite_*``, ``CompositeFaultProblem``) is one problem whose fault
components are the addressable faults: ``fault`` indexes them for ``oracle`` and ``recover``.

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

from sregym.conductor.problems.lifecycle import (
    PROTOCOL_VERSION,
    SEED_RECOVERY_CONTRACT,
    InjectionResult,
    RecoveryFailure,
    RecoveryResult,
    run_recovery,
)

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
        #: The problems injected since the last plain ``inject``; ``compose`` injections join them.
        self._incidents: list[_Problem] = []
        #: The addressable faults, in injection order: (incident, component). A plain problem is one
        #: fault (component ``None``); a registry composite contributes each of its fault components.
        self._faults: list[tuple[int, int | None]] = []
        self._recovered: set[int] = set()
        self._proxy: dict[str, Any] | None = None
        self._components: list[dict[str, Any]] = []
        self._invalid: set[int] = set()
        self._recovery_failed = False
        self._refresh_failed = False
        self._cleanup_errors: dict[int, list[dict[str, Any]]] = {}
        self._recovery_timeouts: dict[int, str] = {}
        self._refresh_timeouts: dict[int, str] = {}
        self._injection_span = 0

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
            "refresh": self._refresh,
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
        """Inject one problem; with ``compose`` it joins the problems injected since the last plain inject.

        The reply's ``faults`` are the indices of its faults in the composition, for ``recover`` and
        ``oracle`` (``fault`` is the first). A plain problem is one fault; a registry composite
        (``CompositeFaultProblem``) contributes one fault per fault component, never its decoys.
        """

        if self._recovery_failed or self._refresh_failed:
            raise RuntimeError("recovery failed; clean up successfully before another injection")
        if self._invalid:
            raise RuntimeError("invalid injection must be recovered before another injection")
        # A fresh instance per incident, as SREGym constructs one per problem run; its oracle
        # snapshots the healthy deployment before the fault.
        problem = self._problem_factory(str(request["problem_id"]))
        if self._proxy is not None:
            # As the conductor does right before ``inject_fault``.
            self._verify_agent_kubeconfig(self._proxy)
        if not request.get("compose"):
            self._incidents, self._faults, self._recovered = [], [], set()
            self._components = []
            self._recovery_timeouts = {}
            self._refresh_timeouts = {}
            self._cleanup_errors = {}
            self._injection_span = 0
        incident = len(self._incidents)
        self._incidents.append(problem)
        first = len(self._faults)
        self._faults.extend((incident, component) for component in _fault_components(problem))
        started = self._clock()
        try:
            problem.inject_fault()
            check = getattr(problem, "injection_failure", lambda: None)
            failure = check()
        except Exception as error:
            failure = {"error": f"{type(error).__name__}: {error}"}
        injection = InjectionResult.from_failure(failure)
        if not injection.valid:
            self._invalid.add(incident)
        describe = getattr(problem, "fault_components", None)
        descriptions = {part["component_index"]: part for part in describe()} if describe else {}
        faults = list(range(first, len(self._faults)))
        for fault in faults:
            _, component = self._faults[fault]
            part = descriptions.get(component, {})
            self._components.append(
                {
                    "fault": fault,
                    "incident": incident,
                    "problem_id": str(request["problem_id"]),
                    "component_index": component,
                    "injection_order": self._injection_span + part.get("injection_order", fault - first),
                    "identity": part.get("identity", {"class": type(problem).__name__}),
                }
            )
        self._injection_span += max(len(getattr(problem, "components", ())), len(faults))
        if hasattr(problem, "pending_components"):
            pending_components = set(problem.pending_components)
            # Catalog injection may already have rolled back, or stopped before later components.
            self._recovered.update(f for f in faults if self._faults[f][1] not in pending_components)
        return {
            "protocol_version": PROTOCOL_VERSION,
            "injection": injection.to_dict(),
            "started_at": started,
            "finished_at": self._clock(),
            "fault": first,
            "faults": faults,
            "components": self._components[first:],
        }

    @property
    def _incident(self) -> _Problem | None:
        return self._incidents[-1] if self._incidents else None

    def _fault(self, request: dict[str, Any]) -> int | None:
        raw = request.get("fault")
        if raw is None:
            return None
        index = int(raw)
        if not 0 <= index < len(self._faults):
            raise ValueError(f"no fault {index}; {len(self._faults)} injected")
        return index

    def _current(self) -> _Problem:
        if self._incident is None:
            raise RuntimeError("no injected incident; send an inject request first")
        return self._incident

    def _grade(self, problem: _Problem, oracle: _Oracle | None = None) -> dict[str, Any]:
        oracle = problem.mitigation_oracle if oracle is None else oracle
        if oracle is None:
            health = self._health_probe(problem.app.namespace)
            return {"kind": "health-check", "success": bool(health.get("healthy")), "details": health}
        details = oracle.evaluate()
        return {"kind": "sregym-mitigation-oracle", "success": bool(details.get("success")), "details": details}

    def _oracle(self, request: dict[str, Any]) -> dict[str, Any]:
        self._current()
        if self._invalid:
            raise RuntimeError("cannot grade an invalid injection")
        if self._recovery_failed or self._refresh_failed:
            raise RuntimeError("cannot grade after failed recovery")
        index = self._fault(request)
        if index is not None:
            incident, component = self._faults[index]
            problem = self._incidents[incident]
            if component is None:
                grade = self._grade(problem)
            else:
                oracle = problem.component_oracle(component)  # type: ignore[attr-defined]
                if oracle is None:
                    raise RuntimeError(f"component {component} has no mitigation oracle")
                grade = self._grade(problem, oracle)
        elif len(self._incidents) == 1:
            grade = self._grade(self._incidents[0])
        else:
            grades = [self._grade(problem) for problem in self._incidents]
            grade = {
                "kind": "composition",
                "success": all(part["success"] for part in grades),
                "details": {"faults": grades},
            }
        return {
            **grade,
            "protocol_version": PROTOCOL_VERSION,
            "components": self._components if index is None else [self._components[index]],
        }

    def _recover(self, request: dict[str, Any]) -> dict[str, Any]:
        """Full warm-lane cleanup; failed or timed-out recoverers are never credited."""
        self._current()
        if request.get("mode", "full") != "full":
            raise ValueError("worker recovery mode must be 'full' (no namespace teardown follows)")
        index = self._fault(request)
        if index is not None and index in self._recovered:
            raise ValueError(f"fault {index} is already recovered")
        started = self._clock()
        pending = (
            [self._faults[index][0]]
            if index is not None
            else [
                incident
                for incident in reversed(range(len(self._incidents)))
                if self._pending(incident) or incident in self._invalid
            ]
        )
        result = RecoveryResult()
        requested: list[int] = []
        for incident in pending:
            problem = self._incidents[incident]
            faults = [
                fault
                for fault in range(len(self._faults))
                if self._faults[fault][0] == incident
                and fault not in self._recovered
                and (index is None or fault == index)
            ]
            faults.sort(key=lambda f: self._components[f]["injection_order"], reverse=True)
            requested.extend(faults)
            component = self._faults[index][1] if index is not None else None
            recover = getattr(problem, "recovery_result", None)
            if recover is not None:
                cleanup = recover(component)
                recovered_components = set(cleanup["recovered"])
                restored = [f for f in faults if self._faults[f][1] in recovered_components]
                errors = cleanup["errors"]
            else:
                # Plain problems and compatible third-party composites also need bounded cleanup.
                # Native composites use recovery_result, including decoys and per-component errors.
                if incident in self._recovery_timeouts:
                    status, error = "timeout", self._recovery_timeouts[incident]
                else:
                    call = (
                        getattr(problem, "recover_to_baseline", problem.recover_fault)
                        if component is None
                        else (lambda problem=problem, component=component: problem.recover_component(component))
                    )
                    status, error = run_recovery(call, getattr(problem, "recover_timeout_s", 240.0))
                    if status == "timeout":
                        self._recovery_timeouts[incident] = error
                restored = faults if status == "recovered" else []
                errors = [] if status == "recovered" else [RecoveryFailure(component, status, error).to_dict()]
            previous_errors = self._cleanup_errors.get(incident, [])
            if index is not None:
                previous_errors = [e for e in previous_errors if e["component_index"] != component]
            else:
                previous_errors = []
            self._cleanup_errors[incident] = previous_errors + errors
            self._recovered.update(restored)
            result.recovered.extend(restored)
            result.errors.extend({"incident": incident, **error} for error in errors)
            if not self._pending(incident):
                self._invalid.discard(incident)
        result.pending = [fault for fault in range(len(self._faults)) if fault not in self._recovered]
        self._recovery_failed = any(self._cleanup_errors.values())
        # Other live faults are expected after a single-component cleanup.
        reply = result.to_dict()
        return {
            **reply,
            "seconds": self._clock() - started,
            "protocol_version": PROTOCOL_VERSION,
            "mode": "full",
            "recovery_contract": SEED_RECOVERY_CONTRACT,
            "components": [self._components[f] for f in requested],
        }

    def _refresh(self, request: dict[str, Any]) -> dict[str, Any]:
        """Refresh runtime clients after full recovery and the caller's seed-manifest restore."""
        self._current()
        if any(self._pending(i) for i in range(len(self._incidents))) or any(self._cleanup_errors.values()):
            raise RuntimeError("client refresh requires successful full recovery")
        result = RecoveryResult()
        for incident in reversed(range(len(self._incidents))):
            problem = self._incidents[incident]
            refresh = getattr(problem, "refresh_after_recovery", None)
            if refresh is None:
                continue
            if incident in self._refresh_timeouts:
                status, error = "timeout", self._refresh_timeouts[incident]
            else:
                status, error = run_recovery(refresh, getattr(problem, "recover_timeout_s", 240.0))
                if status == "timeout":
                    self._refresh_timeouts[incident] = error
            if status != "recovered":
                result.errors.append({"incident": incident, **RecoveryFailure(None, status, error).to_dict()})
        self._refresh_failed = bool(result.errors)
        return {**result.to_dict(), "protocol_version": PROTOCOL_VERSION, "recovery_contract": SEED_RECOVERY_CONTRACT}

    def _pending(self, incident: int) -> bool:
        problem = self._incidents[incident]
        if hasattr(problem, "pending_components"):
            return bool(problem.pending_components)
        return any(fault not in self._recovered for fault, (owner, _) in enumerate(self._faults) if owner == incident)


def _fault_components(problem: _Problem) -> list[int | None]:
    """The addressable faults of one problem: a registry composite's fault components, else itself."""

    indices = getattr(problem, "fault_indices", None)
    return [None] if indices is None else list(indices)


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
