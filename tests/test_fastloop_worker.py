"""Contract tests for the fast inner-loop worker (no cluster, no conductor)."""

import io
import json
from dataclasses import dataclass, field

import pytest

from sregym.fastloop.worker import FastloopWorker, serve


@dataclass
class FakeApp:
    namespace: str = "hotel-reservation"
    app_name: str = "Hotel Reservation"
    description: str = "A hotel reservation app."


@dataclass
class FakeOracle:
    success: bool = True
    calls: int = 0

    def evaluate(self) -> dict:
        self.calls += 1
        return {"success": self.success}


@dataclass
class FakeProblem:
    app: FakeApp = field(default_factory=FakeApp)
    mitigation_oracle: FakeOracle | None = field(default_factory=FakeOracle)
    events: list[str] = field(default_factory=list)

    @property
    def namespace(self) -> str:
        return self.app.namespace

    def inject_fault(self) -> None:
        self.events.append("inject")

    def recover_fault(self) -> None:
        self.events.append("recover")


@dataclass
class Fakes:
    problems: list[FakeProblem] = field(default_factory=list)
    deployed: list[str] = field(default_factory=list)
    healthy: bool = False
    clusters: list[int] = field(default_factory=list)

    def problem(self, problem_id: str) -> FakeProblem:
        if problem_id == "unknown":
            raise KeyError(problem_id)
        created = FakeProblem()
        self.problems.append(created)
        return created

    def deploy(self, problem: FakeProblem, baseline_path: str) -> None:
        self.deployed.append(baseline_path)
        self.healthy = True

    def health(self, namespace: str) -> dict:
        return {"healthy": self.healthy, "namespace": namespace, "unready": [] if self.healthy else ["mongodb-geo"]}

    def cluster(self, worker_id: int, log_dir: str) -> tuple[str, str]:
        self.clusters.append(worker_id)
        return f"fastloop-w{worker_id}", f"{log_dir}/worker_{worker_id}.kubeconfig"


@dataclass
class FakeProxy:
    hidden_namespaces: set[str]
    listen_port: int
    started: bool = False

    def start(self) -> None:
        self.started = True

    def generate_agent_kubeconfig(self, output_path: str | None = None) -> str:
        return output_path or "/tmp/agent-kubeconfig"


def _worker(
    fakes: Fakes,
    proxies: list[FakeProxy] | None = None,
    verified: list[tuple[str, int]] | None = None,
) -> FastloopWorker:
    created = proxies if proxies is not None else []
    checks = verified if verified is not None else []

    def proxy_factory(hidden_namespaces: set[str], listen_port: int) -> FakeProxy:
        proxy = FakeProxy(hidden_namespaces=hidden_namespaces, listen_port=listen_port)
        created.append(proxy)
        return proxy

    return FastloopWorker(
        problem_factory=fakes.problem,
        deployer=fakes.deploy,
        health_probe=fakes.health,
        cluster_factory=fakes.cluster,
        proxy_factory=proxy_factory,
        prompt_builder=lambda app_info, api_base: f"{app_info['app_name']} in {app_info['namespace']} via {api_base}",
        kubeconfig_verifier=lambda path, port: checks.append((path, port)),
    )


def test_deploy_is_skipped_while_the_app_is_healthy() -> None:
    fakes = Fakes()
    worker = _worker(fakes)

    first = worker.handle({"op": "deploy", "problem_id": "p", "baseline_path": "/b.json"})
    second = worker.handle({"op": "deploy", "problem_id": "p", "baseline_path": "/b.json"})
    forced = worker.handle({"op": "deploy", "problem_id": "p", "baseline_path": "/b.json", "redeploy": True})

    assert (first["deployed"], second["deployed"], forced["deployed"]) == (True, False, True)
    assert fakes.deployed == ["/b.json", "/b.json"]


def test_each_incident_uses_a_fresh_problem_that_recovers_and_grades_its_own_fault() -> None:
    fakes = Fakes(healthy=True)
    worker = _worker(fakes)

    injected = worker.handle({"op": "inject", "problem_id": "p"})
    oracle = worker.handle({"op": "oracle"})
    worker.handle({"op": "recover"})
    worker.handle({"op": "inject", "problem_id": "p"})

    assert injected["started_at"] <= injected["finished_at"]
    assert oracle == {"kind": "sregym-mitigation-oracle", "success": True, "details": {"success": True}}
    assert [problem.events for problem in fakes.problems] == [["inject", "recover"], ["inject"]]


def test_problem_without_a_mitigation_oracle_is_graded_by_a_health_check() -> None:
    fakes = Fakes(healthy=False)
    worker = _worker(fakes)
    worker.handle({"op": "inject", "problem_id": "p"})
    fakes.problems[0].mitigation_oracle = None

    oracle = worker.handle({"op": "oracle"})

    assert oracle["kind"] == "health-check"
    assert oracle["success"] is False
    assert oracle["details"]["unready"] == ["mongodb-geo"]


def test_recover_and_oracle_require_an_injected_incident() -> None:
    worker = _worker(Fakes())

    with pytest.raises(RuntimeError, match="no injected incident"):
        worker.handle({"op": "recover"})
    with pytest.raises(RuntimeError, match="no injected incident"):
        worker.handle({"op": "oracle"})


def test_app_info_matches_what_the_conductor_serves_to_agents() -> None:
    worker = _worker(Fakes())

    info = worker.handle({"op": "app_info", "problem_id": "p"})

    assert info == {
        "app_name": "Hotel Reservation",
        "namespace": "hotel-reservation",
        "descriptions": "A hotel reservation app.",
    }


def test_serve_speaks_json_lines_and_reports_errors_without_dying() -> None:
    fakes = Fakes()
    requests = io.StringIO(
        "\n".join(
            [
                json.dumps({"id": 1, "op": "cluster", "worker_id": 0, "log_dir": "/logs"}),
                json.dumps({"id": 2, "op": "inject", "problem_id": "unknown"}),
                "not json",
                json.dumps({"id": 3, "op": "nope"}),
                json.dumps({"id": 4, "op": "shutdown"}),
                json.dumps({"id": 5, "op": "cluster", "worker_id": 1, "log_dir": "/logs"}),
            ]
        )
        + "\n"
    )
    responses = io.StringIO()

    serve(_worker(fakes), requests, responses)

    replies = [json.loads(line) for line in responses.getvalue().splitlines()]
    assert replies[0] == {
        "id": 1,
        "ok": True,
        "result": {"cluster": "fastloop-w0", "kubeconfig": "/logs/worker_0.kubeconfig"},
    }
    assert replies[1]["id"] == 2 and replies[1]["ok"] is False and "unknown" in replies[1]["error"]
    assert replies[2]["ok"] is False and replies[2]["id"] is None
    assert replies[3]["ok"] is False and "nope" in replies[3]["error"]
    assert replies[4] == {"id": 4, "ok": True, "result": {}}
    # Nothing after shutdown is served.
    assert len(replies) == 5
    assert fakes.clusters == [0]


def test_agent_proxy_hides_benchmark_namespaces_plus_the_requested_ones_and_starts_once() -> None:
    proxies: list[FakeProxy] = []
    worker = _worker(Fakes(), proxies)

    first = worker.handle({"op": "proxy", "port": 26443, "hide": ["hotel-reservation-sdo"], "kubeconfig": "/k"})
    second = worker.handle({"op": "proxy", "port": 26443, "hide": ["hotel-reservation-sdo"], "kubeconfig": "/k"})

    assert first == second == {"kubeconfig": "/k", "port": 26443}
    assert len(proxies) == 1 and proxies[0].started
    assert proxies[0].hidden_namespaces == {"chaos-mesh", "khaos", "hotel-reservation-sdo"}


def test_codex_prompt_is_built_by_the_benchmark_client_for_the_given_endpoint() -> None:
    worker = _worker(Fakes())

    prompt = worker.handle({"op": "codex_prompt", "problem_id": "p", "api_base": "http://127.0.0.1:18765"})

    assert prompt == {"prompt": "Hotel Reservation in hotel-reservation via http://127.0.0.1:18765"}


def test_agent_kubeconfig_is_verified_at_proxy_start_and_again_before_each_injection() -> None:
    verified: list[tuple[str, int]] = []
    worker = _worker(Fakes(), verified=verified)

    worker.handle({"op": "inject", "problem_id": "p"})
    assert verified == []  # no agent proxy yet, nothing handed to an agent

    worker.handle({"op": "proxy", "port": 26443, "kubeconfig": "/k"})
    worker.handle({"op": "proxy", "port": 26443, "kubeconfig": "/k"})
    worker.handle({"op": "inject", "problem_id": "p"})

    assert verified == [("/k", 26443), ("/k", 26443)]


def test_a_crossed_agent_kubeconfig_fails_the_proxy_request_and_is_not_handed_out() -> None:
    def crossed(path: str, port: int) -> None:
        raise RuntimeError(f"{path} does not reach fastloop-w0 only")

    worker = FastloopWorker(
        problem_factory=Fakes().problem,
        deployer=Fakes().deploy,
        health_probe=Fakes().health,
        cluster_factory=Fakes().cluster,
        proxy_factory=lambda hidden, port: FakeProxy(hidden_namespaces=hidden, listen_port=port),
        kubeconfig_verifier=crossed,
    )
    replies = io.StringIO()

    serve(worker, io.StringIO(json.dumps({"id": 1, "op": "proxy", "port": 26443, "kubeconfig": "/k"}) + "\n"), replies)

    reply = json.loads(replies.getvalue())
    assert reply["ok"] is False and "fastloop-w0 only" in reply["error"]


def test_production_deploy_persists_the_baseline_to_the_requested_per_run_file(monkeypatch, tmp_path) -> None:
    import sregym.conductor.conductor as conductor_module
    from sregym.fastloop import worker as worker_module

    seen: list[object] = []

    class RecordingConductor:
        def __init__(self, config) -> None:
            self.mcp_server = type("Mcp", (), {"deploy": lambda self: None})()

        def deploy_app(self) -> None:
            seen.append(conductor_module.cluster_baseline_state_file())

    monkeypatch.setattr(conductor_module, "Conductor", RecordingConductor)
    # _deploy rebinds the conductor's baseline-path function; restore it after the test.
    monkeypatch.setattr(conductor_module, "cluster_baseline_state_file", conductor_module.cluster_baseline_state_file)
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    baseline = tmp_path / "cluster_baseline_state.json"

    worker_module._deploy(FakeProblem(), str(baseline))

    assert seen == [baseline]


def test_the_worker_holds_its_cluster_lock_so_a_second_driver_fails_loudly(monkeypatch, tmp_path) -> None:
    import sregym.worker_infra as worker_infra
    from sregym.fastloop.worker import cluster_lock_from_environment

    monkeypatch.setattr(worker_infra, "_LOCK_DIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")

    with (
        cluster_lock_from_environment(),
        pytest.raises(worker_infra.ClusterInUseError),
        worker_infra.cluster_lock("fastloop-w0"),
    ):
        pass
    with worker_infra.cluster_lock("fastloop-w0"):
        pass  # released with the worker


def test_without_a_cluster_name_the_worker_takes_no_lock(monkeypatch, tmp_path) -> None:
    import sregym.worker_infra as worker_infra
    from sregym.fastloop.worker import cluster_lock_from_environment

    monkeypatch.setattr(worker_infra, "_LOCK_DIR", str(tmp_path))
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)

    with cluster_lock_from_environment():
        pass

    assert list(tmp_path.iterdir()) == []
