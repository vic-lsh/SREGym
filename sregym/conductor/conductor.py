import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path

import yaml

from sregym.conductor.constants import StartProblemResult
from sregym.conductor.oracles.detection import DetectionOracle
from sregym.conductor.oracles.diagnosis_oracle import DiagnosisOracle
from sregym.conductor.problems.registry import ProblemRegistry
from sregym.conductor.utils import is_ordered_subset
from sregym.generators.fault.inject_remote_os import RemoteOSFaultInjector
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.generators.noise.manager import get_noise_manager
from sregym.service.apps.app_registry import AppRegistry
from sregym.service.cluster_state import ClusterStateManager
from sregym.service.dm_dust_manager import DmDustManager
from sregym.service.dm_flakey_manager import DmFlakeyManager
from sregym.service.k8s_proxy import KubernetesAPIProxy
from sregym.service.khaos import KhaosController
from sregym.service.kubeconfig import require_kubeconfig_path
from sregym.service.kubectl import KubeCtl
from sregym.service.telemetry.prometheus import Prometheus


_DEFAULT_CLEANUP_DEFER_TIMEOUT_SECONDS = 600.0


class Conductor:
    def __init__(self, tasklist_path: str | None = None, defer_cleanup: bool = False):
        self.base_kubeconfig = require_kubeconfig_path()
        self._tasklist_path = tasklist_path

        # Opt-in per-agent via `defer_cleanup: true` in agents.yaml. When True,
        # after the final stage is evaluated the conductor transitions to
        # "awaiting_cleanup" instead of running teardown. This lets agents that
        # do post-submit reflection (e.g. crucible: recovery-diagnosis + playbook
        # generation) inspect the live cluster before it is torn down.
        # Teardown runs on an explicit POST /cleanup from the agent, on the
        # driver's crash-path force_cleanup(), or on the watchdog timer — whichever
        # fires first. All paths serialize on self._cleanup_lock for exactly-once.
        self._defer_cleanup = defer_cleanup
        # Serializes teardown so any combination of /cleanup, driver crash-path, and
        # watchdog timer invokes _run_teardown() exactly once.
        self._cleanup_lock = threading.Lock()
        self._cleanup_timer: threading.Timer | None = None

        # core services
        self.problems = ProblemRegistry()
        self.kubectl = KubeCtl(kubeconfig_path=self.base_kubeconfig)
        self.prometheus = Prometheus(kubeconfig_path=self.base_kubeconfig)
        self.apps = AppRegistry()
        self.agent_name = None

        self.khaos = KhaosController(self.kubectl)
        self.dm_dust_manager = DmDustManager(self.kubectl)
        self.dm_flakey_manager = DmFlakeyManager(self.kubectl)
        self.cluster_state = ClusterStateManager(self.kubectl)
        self._baseline_captured = False

        # Kubernetes API proxy to hide chaos engineering namespaces from agents
        proxy_port = 16443
        worker_id = os.getenv("SREGYM_WORKER_ID")
        if worker_id:
            proxy_port += int(worker_id)

        self.k8s_proxy = KubernetesAPIProxy(
            hidden_namespaces={"chaos-mesh", "khaos"},
            listen_port=proxy_port,
            kubeconfig_path=self.base_kubeconfig,
        )
        self._agent_kubeconfig_path: str | None = None

        self.problem = None
        self.detection_oracle = None
        self.problem_id = None
        self.problem = None
        self.app = None
        self.detection_oracle = None
        self.execution_start_time = None

        # grading flow state
        # submission_stage reflects the current stage (e.g., "diagnosis", "mitigation") or "done"
        self.submission_stage = None
        self.results = {}

        # Autonomous-mode deferred diagnosis grading: accumulate all submit_diagnosis
        # calls during the episode; grade holistically in force_cleanup after agent exits.
        self.diagnosis_submissions: list[str] = []

        self.tasklist = None
        self.logger = logging.getLogger("all.sregym.conductor")

        self.stage_sequence: list[dict] = []
        self.current_stage_index: int = 0
        self.waiting_for_agent: bool = False
        self.fault_injected: bool = False
        self.status_callback = None

    def set_status_callback(self, callback):
        self.status_callback = callback

    def register_agent(self, name="agent"):
        self.agent_name = name

    def start_k8s_proxy(self):
        """
        Start the Kubernetes API proxy that hides chaos engineering namespaces.
        Should be called before launching agents.
        """
        self.logger.info("Starting Kubernetes API filtering proxy...")
        self.k8s_proxy.start()
        worker_id = os.getenv("SREGYM_WORKER_ID", "main")
        kubeconfig_path = os.path.join(
            tempfile.gettempdir(), f"sregym-agent-kubeconfig-w{worker_id}-p{self.k8s_proxy.listen_port}"
        )
        self._agent_kubeconfig_path = self.k8s_proxy.generate_agent_kubeconfig(output_path=kubeconfig_path)
        self.logger.info(f"Agent kubeconfig generated at: {self._agent_kubeconfig_path}")

    def stop_k8s_proxy(self):
        """Stop the Kubernetes API proxy."""
        self.logger.info("Stopping Kubernetes API filtering proxy...")
        self.k8s_proxy.stop()
        self._agent_kubeconfig_path = None

    def get_agent_kubeconfig_path(self) -> str | None:
        """
        Get the path to the kubeconfig file that agents should use.
        This kubeconfig points to the filtering proxy that hides chaos namespaces.
        """
        return self._agent_kubeconfig_path

    def dependency_check(self, binaries: list[str]):
        for b in binaries:
            if shutil.which(b) is None:
                self.logger.error(f"Required dependency '{b}' not found.")
                raise RuntimeError(f"[❌] Required dependency '{b}' not found.")

    def get_problem_stages(self):
        if self._tasklist_path:
            tasklist_path = Path(self._tasklist_path)
        else:
            file_dir = Path(__file__).resolve().parent
            tasklist_path = file_dir / "tasklist.yml"

        # If tasklist file doesn't exist, default to running diagnosis + mitigation
        if not tasklist_path.exists():
            self.logger.info("No tasklist.yml found. Defaulting to running diagnosis and mitigation for this problem.")
            self.tasklist = ["diagnosis", "mitigation"]
            return

        with open(tasklist_path, "r") as f:
            tasklist = yaml.safe_load(f)
            if not tasklist:
                msg = "Badly formatted tasklist.yml"
                self.logger.error(msg)
                raise RuntimeError(msg)
            problems = tasklist["all"]["problems"]

        if self.problem_id not in (problems if problems else []):
            self.logger.warning("problem_id not found in tasklist. Defaulting to running diagnosis and mitigation.")
            self.tasklist = ["diagnosis", "mitigation"]
        else:
            problem_tasklist = problems[self.problem_id]
            if not problem_tasklist:
                msg = f"No tasks specified for {self.problem_id}"
                self.logger.error(msg)
                raise RuntimeError(msg)

            if not is_ordered_subset(problem_tasklist, ["diagnosis", "mitigation"]):
                msg = f"Task list for {self.problem_id} is either out of order or has an unknown step (allowed: diagnosis, mitigation)"
                self.logger.error(msg)
                raise RuntimeError(msg)

            self.logger.info(f"Tasklist specified for {self.problem_id}. Configured stages to run: {problem_tasklist}")

            # Use the tasklist as-is (stage names: diagnosis, mitigation)
            self.tasklist = problem_tasklist

    def _build_stage_sequence(self):
        """Build the sequence of stages (diagnosis, mitigation) based on tasklist and available oracles."""
        self.stage_sequence = []
        self.current_stage_index = 0
        self.waiting_for_agent = False
        self.fault_injected = False

        if not self.tasklist:
            self.logger.warning("Empty tasklist; no stages configured for this problem.")
            return

        # Map stage names to their evaluation functions
        stage_definitions = {
            "diagnosis": self._evaluate_diagnosis,
            "mitigation": self._evaluate_mitigation,
        }

        # Determine which stages are actually available (oracle attached)
        for name in self.tasklist:
            if name not in stage_definitions:
                self.logger.warning(f"Unknown stage '{name}' in tasklist; skipping.")
                continue

            if name == "diagnosis":
                if getattr(self.problem, "diagnosis_oracle", None):
                    self.stage_sequence.append(
                        {
                            "name": name,
                            "evaluation": stage_definitions[name],
                        }
                    )
                else:
                    self.logger.info("⏩ Diagnosis oracle is not attached. Skipping diagnosis.")

            elif name == "mitigation":
                if getattr(self.problem, "mitigation_oracle", None):
                    self.stage_sequence.append(
                        {
                            "name": name,
                            "evaluation": stage_definitions[name],
                        }
                    )
                else:
                    self.logger.info("⏩ Mitigation oracle is not attached. Skipping mitigation.")

        if not self.stage_sequence:
            self.logger.warning(
                "No stages left after checking oracles. This problem will complete without agent interaction."
            )

    def _inject_fault(self):
        """Inject fault and prepare diagnosis checkpoint if available."""
        if self.status_callback:
            self.status_callback("Injecting Faults")

        self.logger.info("[ENV] Starting fault injection...")
        self.problem.inject_fault()
        self.problem.verify_fault_applied()
        self.logger.info("[ENV] Injected fault")
        self.fault_injected = True

        # Prepare diagnosis checkpoint if available, after fault injection but before agent stages
        if (
            hasattr(self.problem, "diagnosis_oracle")
            and self.problem.diagnosis_oracle
            and isinstance(self.problem.diagnosis_oracle, DiagnosisOracle)
        ):
            self.problem.diagnosis_oracle.load_diagnosis_checkpoint()
            self.logger.info("Diagnosis checkpoint loaded after fault injection.")

    def _evaluate_diagnosis(self, solution):
        """Evaluation logic for diagnosis stage."""
        self.logger.info("Start Eval for Diagnosis", extra={"sol": solution})
        r = self.problem.diagnosis_oracle.evaluate(solution)
        self.results["Diagnosis"] = r
        self.results["TTL"] = time.time() - self.execution_start_time
        self.logger.info(
            f"[EVAL] Diagnosis "
            f"{'Succeed' if self.results['Diagnosis']['success'] else 'Failed'}\n "
            f"TTL: {self.results['TTL']}"
        )
        return r

    def _evaluate_mitigation(self, solution):
        """Evaluation logic for mitigation stage."""
        # Currently mitigation_oracle.evaluate() does not take the agent solution directly.
        self.logger.info("Start Eval for Mitigation", extra={"sol": solution})
        try:
            get_noise_manager().stop()
        except Exception as e:
            self.logger.warning(f"Failed to stop NoiseManager before mitigation eval: {e}")
        r = self.problem.mitigation_oracle.evaluate()
        self.results["Mitigation"] = r
        self.results["TTM"] = time.time() - self.execution_start_time
        self.logger.info(
            f"[EVAL] Mitigation "
            f"{'Succeed' if self.results['Mitigation']['success'] else 'Failed'}\n "
            f"TTM: {self.results['TTM']}"
        )
        return r

    def _advance_to_next_stage(self, start_index: int = 0):
        """
        Advance to the next stage starting from start_index.
        If there are more stages, set up for agent submission.
        Otherwise, finish the problem.
        """
        self.waiting_for_agent = False
        self.current_stage_index = start_index

        if not self.stage_sequence:
            self.logger.info("No stages configured; finishing problem immediately.")
            self._finish_problem()
            return

        # Inject fault before the first stage if not already done
        if start_index == 0 and not self.fault_injected:
            self._inject_fault()
            self.execution_start_time = time.time()

        if start_index < len(self.stage_sequence):
            stage = self.stage_sequence[start_index]
            stage_name = stage.get("name")

            self.logger.debug(f"Advancing to stage '{stage_name}' and waiting for agent.")
            self.waiting_for_agent = True
            self.submission_stage = stage_name
            self.logger.info(f"[STAGE] Go to stage {self.submission_stage}")

            # Update NoiseManager stage
            try:
                nm = get_noise_manager()
                nm.set_stage(self.submission_stage)
            except Exception as e:
                self.logger.warning(f"Failed to set NoiseManager stage: {e}")
        else:
            # No more stages. If deferred cleanup is enabled, gate teardown on an
            # explicit signal (POST /cleanup, driver crash-path, or watchdog) so the
            # agent can run post-submit reflection against a live cluster.
            if self._defer_cleanup:
                self.submission_stage = "awaiting_cleanup"
                self.logger.info("[STAGE] Awaiting cleanup signal from agent")
                self._start_cleanup_watchdog()
            else:
                self._finish_problem()

    def _run_teardown(self):
        """Synchronously recover fault, undeploy app, reconcile cluster state.

        Not idempotent on its own — must be called through force_cleanup()
        or _finish_problem(), both of which serialize on self._cleanup_lock.
        """
        self.logger.info("[STAGE] Done, recover fault")

        # Stop noises
        try:
            nm = get_noise_manager()
            nm.stop()
        except Exception as e:
            self.logger.warning(f"Failed to stop NoiseManager: {e}")

        if self.problem:
            self.problem.recover_fault()

        self.logger.info("[STAGE] Undeploy app")
        self.undeploy_app()

        # Reconcile cluster state to baseline to clean up any changes made by the agent
        if self._baseline_captured:
            self.logger.info("[STAGE] Reconciling cluster state to baseline")
            try:
                changes = self.cluster_state.reconcile_to_baseline()
                if any(v for v in changes.values() if v):
                    self.logger.info(f"Cluster state reconciliation changes: {changes}")
            except Exception as e:
                self.logger.warning(f"Failed to reconcile cluster state: {e}")

    def _finish_problem(self):
        with self._cleanup_lock:
            if self.submission_stage == "done":
                return
            self._cancel_cleanup_watchdog()
            self._run_teardown()
            # Set to "done" after all cleanup is complete to prevent race condition
            # where the next problem starts before cleanup finishes
            self.submission_stage = "done"

    def _evaluate_diagnosis_deferred(self):
        """Grade accumulated autonomous-mode diagnosis submissions after agent exits.

        Concatenates all submissions and calls the problem's diagnosis oracle.
        Only runs when at least one submission was collected and the oracle exists.
        """
        if not self.diagnosis_submissions:
            self.logger.warning("Deferred diagnosis grading: no submissions collected.")
            self.results["Diagnosis"] = {"success": False, "accuracy": 0.0}
            return
        if not getattr(self.problem, "diagnosis_oracle", None):
            self.logger.warning("Deferred diagnosis grading: no diagnosis oracle on problem.")
            return

        combined = "\n".join(self.diagnosis_submissions)
        self.logger.info(
            f"Deferred diagnosis grading: evaluating {len(self.diagnosis_submissions)} "
            f"submission(s) combined into {len(combined)} chars."
        )
        r = self.problem.diagnosis_oracle.evaluate(combined)
        r["deferred"] = True
        r["num_submissions"] = len(self.diagnosis_submissions)
        self.results["Diagnosis"] = r
        self.results["TTL"] = time.time() - self.execution_start_time
        self.logger.info(
            f"[EVAL] Deferred Diagnosis "
            f"{'Succeeded' if r.get('success') else 'Failed'} | "
            f"accuracy={r.get('accuracy', 0.0):.1f}"
        )

    def force_cleanup(self):
        """Public entry used by the POST /cleanup handler, the driver's crash
        path, and the watchdog timer. Idempotent: safe to call from multiple
        call sites and stages."""
        with self._cleanup_lock:
            if self.submission_stage == "done":
                return
            if self.submission_stage != "awaiting_cleanup":
                self.logger.warning(
                    f"force_cleanup called at unexpected stage {self.submission_stage!r}; "
                    "running teardown anyway to guarantee cluster cleanup."
                )
            if self.diagnosis_submissions and "Diagnosis" not in self.results:
                self._evaluate_diagnosis_deferred()
            self._cancel_cleanup_watchdog()
            self._run_teardown()
            self.submission_stage = "done"

    def _start_cleanup_watchdog(self):
        timeout_env = os.getenv("SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS")
        try:
            timeout = float(timeout_env) if timeout_env else _DEFAULT_CLEANUP_DEFER_TIMEOUT_SECONDS
        except ValueError:
            timeout = _DEFAULT_CLEANUP_DEFER_TIMEOUT_SECONDS
        self._cancel_cleanup_watchdog()
        timer = threading.Timer(timeout, self._cleanup_watchdog_fired, args=(timeout,))
        timer.daemon = True
        self._cleanup_timer = timer
        timer.start()

    def _cancel_cleanup_watchdog(self):
        timer = self._cleanup_timer
        if timer is not None:
            timer.cancel()
            self._cleanup_timer = None

    def _cleanup_watchdog_fired(self, timeout: float):
        self.logger.warning(
            f"Cleanup deferral watchdog fired after {timeout:.0f}s; running teardown."
        )
        try:
            self.force_cleanup()
        except Exception as e:
            self.logger.exception(f"Watchdog-triggered force_cleanup failed: {e}")

    async def start_problem(self) -> StartProblemResult:
        """
        1) Provision infra & workload
        2) Initialize Act registry and execute initial GymActs and first AgentAct precondition

        Returns:
            StartProblemResult: Result status indicating success or skip reason
        """
        self.problem = self.problems.get_problem_instance(self.problem_id)
        self.app = self.problem.app
        self.detection_oracle = DetectionOracle(self.problem)
        self.results = {}
        self.diagnosis_submissions = []

        self.dependency_check(["kubectl", "helm"])
        self.logger.debug("Dependency check passed: kubectl, helm")

        self.logger.info(f"[Session Start] Problem ID: {self.problem_id}")
        self.logger.info(f"[STAGE] Start testing on problem: {self.problem_id}")

        if self.problem.requires_khaos() and self.kubectl.is_emulated_cluster():
            self.logger.warning(
                f"Problem '{self.problem_id}' requires Khaos for eBPF-based fault injection, "
                "but Khaos cannot be deployed on emulated clusters (kind, minikube, k3d, etc.). "
                "Skipping this problem."
            )
            return StartProblemResult.SKIPPED_KHAOS_REQUIRED

        self.fix_kubernetes()

        self.get_problem_stages()
        self._build_stage_sequence()

        self.logger.info("Undeploying app leftovers...")
        self.undeploy_app()  # Cleanup any leftovers
        self.logger.info("App leftovers undeployed.")

        # Reconcile cluster-scoped resources (ClusterRoles, CRDs, etc.) that may
        # have leaked from a previous failed helm install on this worker.
        if self._baseline_captured:
            try:
                changes = self.cluster_state.reconcile_to_baseline()
                if any(v for v in changes.values() if v):
                    self.logger.info(f"Pre-deploy reconciliation changes: {changes}")
            except Exception as e:
                self.logger.warning(f"Pre-deploy cluster reconciliation failed: {e}")

        self.logger.info("Deploying app...")
        if self.status_callback:
            self.status_callback("Deploying App")
        self.deploy_app()
        self.logger.info("App deployed.")

        # Update NoiseManager with problem context
        try:
            nm = get_noise_manager()
            context = {
                "namespace": self.app.namespace,
                "app_name": self.app.name,
                # We can add more info here if needed, e.g. service list
            }
            nm.set_problem_context(context)
            nm.start_background_noises()
        except Exception as e:
            self.logger.warning(f"Failed to update NoiseManager context: {e}")

        # After deployment, advance to the first stage
        self._advance_to_next_stage(start_index=0)

        if self.submission_stage and self.submission_stage != "done":
            self.logger.info(f"✅ Deployment complete. Ready for submission. Current stage is: {self.submission_stage}")
        else:
            self.logger.info(
                "✅ Deployment complete. No stages configured; problem will complete without agent submission."
            )
        return StartProblemResult.SUCCESS

    async def submit(self, wrapped_cmd: str) -> dict:
        """
        Called by CLI or HTTP /submit.  Parses & grades the `submit(...)` call,
        advances submission_stage, records results—and when we hit "done",
        triggers undeploy_app. Returns a snapshot of the results dict.
        """
        from sregym.conductor.parser import ResponseParser

        parser = ResponseParser()
        parsed = parser.parse(wrapped_cmd)
        if parsed["api_name"] != "submit":
            raise ValueError("Only `submit(...)` is supported.")
        sol = parsed["args"][0] if parsed["args"] else None

        # If all tasks are already completed, simply return the final snapshot.
        if self.submission_stage == "done":
            self.logger.info("All tasks already completed; ignoring new submission.")
            return dict(self.results)

        if not self.stage_sequence:
            self.logger.warning("submit() called but no stages are configured; returning current results.")
            return dict(self.results)

        if not self.waiting_for_agent:
            self.logger.error(
                "submit() called when conductor is not waiting for a submission. "
                f"Current submission_stage={self.submission_stage}"
            )
            raise RuntimeError("Conductor is not currently waiting for an agent submission.")

        current_stage = self.stage_sequence[self.current_stage_index]
        stage_name = current_stage.get("name")
        self.logger.info(f"Evaluating stage '{stage_name}'", extra={"sol": sol})

        # Indicate verification in progress (this allows UI to show "Verifying" state)
        self.submission_stage = f"{stage_name} (verifying)"

        # Stop noise before evaluation to ensure clean environment
        try:
            nm = get_noise_manager()
            self.logger.info("Stopping noise manager before evaluation...")
            nm.stop()
        except Exception as e:
            self.logger.warning(f"Failed to stop noise manager: {e}")

        # Run the evaluation function for the current stage
        current_stage["evaluation"](sol)

        # After evaluation, advance to the next stage (if any)
        next_index = self.current_stage_index + 1
        self._advance_to_next_stage(start_index=next_index)

        # Restart noise only when advancing to another real stage. Do NOT restart while
        # in "awaiting_cleanup" (deferred-teardown gate) or after "done" (teardown ran).
        if self.submission_stage not in {"done", "awaiting_cleanup"}:
            try:
                nm = get_noise_manager()
                self.logger.info("Restarting noise manager for next stage...")
                nm.start_background_noises()
            except Exception as e:
                self.logger.warning(f"Failed to restart noise manager: {e}")

        return dict(self.results)

    def fix_kubernetes(self):
        self.logger.info("Fixing Kubernetes... to normal state.")
        self.logger.info("[FIX] Imbalance leftover if any")

        injector = VirtualizationFaultInjector(namespace="kube-system")
        injector.recover_daemon_set_image_replacement(
            daemon_set_name="kube-proxy", original_image="registry.k8s.io/kube-proxy:v1.31.13"
        )

        self.logger.info("[FIX] KubeletCrash leftover if any")
        injector = RemoteOSFaultInjector()
        injector.recover_kubelet_crash()
        self.logger.info("Fix Kubernetes completed.")

    def _configure_openebs_image_pull_secret(self):
        """Attach a Docker Hub imagePullSecret to OpenEBS service accounts when available."""
        enabled = os.getenv("SREGYM_ENABLE_DOCKERHUB_PULL_SECRET", "1").strip().lower()
        if enabled in {"0", "false", "no"}:
            self.logger.info("[DEPLOY] Docker Hub imagePullSecret injection disabled.")
            return

        docker_config_path = os.getenv("SREGYM_DOCKER_CONFIG_JSON", os.path.expanduser("~/.docker/config.json"))
        if not os.path.exists(docker_config_path):
            self.logger.warning(
                f"[DEPLOY] Docker config not found at {docker_config_path}; skipping OpenEBS imagePullSecret setup."
            )
            return

        secret_name = os.getenv("SREGYM_DOCKER_PULL_SECRET_NAME", "dockerhub-creds")
        escaped_path = docker_config_path.replace("'", "'\"'\"'")

        self.kubectl.exec_command(
            "kubectl -n openebs create secret generic "
            f"{secret_name} --type=kubernetes.io/dockerconfigjson "
            f"--from-file=.dockerconfigjson='{escaped_path}' --dry-run=client -o yaml | kubectl apply -f -"
        )

        service_accounts = self.kubectl.exec_command(
            "kubectl -n openebs get sa -o jsonpath='{.items[*].metadata.name}'"
        ).strip()
        if not service_accounts:
            self.logger.warning("[DEPLOY] No OpenEBS service accounts found to patch imagePullSecrets.")
            return

        for sa_name in service_accounts.split():
            self.kubectl.exec_command(
                "kubectl -n openebs patch sa "
                f"{sa_name} --type=merge -p "
                f'\'{{"imagePullSecrets":[{{"name":"{secret_name}"}}]}}\''
            )

        # Restart OpenEBS pods so existing replicas pick up patched service accounts.
        self.kubectl.exec_command("kubectl -n openebs delete pod --all --ignore-not-found")
        self.logger.info(f"[DEPLOY] Configured OpenEBS imagePullSecrets using '{secret_name}'.")

    def deploy_app(self):
        """Kubectl + Prometheus + problem.app deployment."""
        self.submission_stage = "setup"
        self.logger.info("[DEPLOY] Setting up metrics-server…")
        self.kubectl.exec_command(
            "kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/"
            "releases/latest/download/components.yaml"
        )
        self.kubectl.exec_command(
            "kubectl -n kube-system patch deployment metrics-server "
            "--type=json -p='["
            '{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"},'
            '{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-preferred-address-types=InternalIP"}'
            "]'"
        )
        self.kubectl.wait_for_ready("kube-system")

        # Only deploy Khaos if the problem requires it
        if self.problem and self.problem.requires_khaos():
            self.logger.info("[DEPLOY] Deploying Khaos DaemonSet...")
            self.khaos.ensure_deployed()

        self.logger.info("[DEPLOY] Setting up OpenEBS…")
        self.kubectl.exec_command("kubectl apply -f https://openebs.github.io/charts/openebs-operator.yaml")
        self.kubectl.exec_command(
            "kubectl patch storageclass openebs-hostpath "
            '-p \'{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}\''
        )
        self._configure_openebs_image_pull_secret()
        self.kubectl.wait_for_ready("openebs")

        print("Setting up OpenEBS LocalPV-Device…")
        device_sc_yaml = """
        apiVersion: storage.k8s.io/v1
        kind: StorageClass
        metadata:
        name: openebs-device
        annotations:
            openebs.io/cas-type: local
        provisioner: openebs.io/local
        parameters:
        localpvType: "device"
        volumeBindingMode: WaitForFirstConsumer
        """
        self.kubectl.exec_command("kubectl apply -f - <<EOF\n" + device_sc_yaml + "\nEOF")

        self.logger.info("[DEPLOY] Deploying Prometheus…")
        self.prometheus.deploy()

        # Set up fault injection infrastructure based on problem type
        # Only one can be active at /var/openebs/local at a time
        problem_name = self.problem.__class__.__name__

        if "LatentSectorError" in problem_name:
            print("Setting up dm-dust infrastructure for LSE fault injection...")
            self.dm_dust_manager.setup_openebs_dm_dust_infrastructure()
        elif "SilentDataCorruption" in problem_name:
            print("Setting up dm-flakey infrastructure for Silent Data Corruption fault injection...")
            self.dm_flakey_manager.setup_openebs_dm_flakey_infrastructure()

        self.logger.info("[ENV] Set up necessary components: metrics-server, Khaos, OpenEBS, Prometheus")

        # Capture cluster baseline state after infrastructure is deployed but before app deployment
        # This allows us to reset the cluster to a clean state after each problem
        if not self._baseline_captured:
            self.logger.info("[DEPLOY] Capturing cluster baseline state...")
            self.cluster_state.capture_baseline()
            self._baseline_captured = True

        self.logger.info("[DEPLOY] Deploying and starting workload")
        self.problem.app.deploy()
        self.logger.info(f"[ENV] Deploy application: {self.problem.app.name}")

        self.problem.app.start_workload()
        self.logger.info("[ENV] Start workload")

    def undeploy_app(self):
        """Teardown problem.app and, if no other apps running, OpenEBS/Prometheus."""
        if self.problem:
            self.problem.app.cleanup()

    def get_deployed_apps(self):
        deployed_apps = []
        for app_name in self.apps.get_app_names():
            namespace = self.apps.get_app_metadata(app_name)["Namespace"]
            if self.kubectl.get_namespace_deployment_status(namespace):
                deployed_apps.append(app_name)

        return deployed_apps
