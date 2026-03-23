import os
import shutil
import signal
import subprocess
import sys
import threading
from datetime import datetime
from typing import Dict, Optional

from .agent_registry import AgentRegistration


class AgentProcess:
    def __init__(self, name: str, proc: subprocess.Popen):
        self.name = name
        self.proc = proc
        self.started_at = datetime.utcnow()


class AgentLauncher:
    def __init__(self):
        self._procs: Dict[str, AgentProcess] = {}
        self._agent_kubeconfig_path: Optional[str] = None

    def set_agent_kubeconfig(self, kubeconfig_path: Optional[str]):
        """
        Set the kubeconfig path that agents should use.
        This is typically the filtered kubeconfig from the K8s proxy.
        """
        self._agent_kubeconfig_path = kubeconfig_path

    async def ensure_started(self, reg: AgentRegistration, extra_args: str = "") -> Optional[AgentProcess]:
        if not reg or not reg.kickoff_command:
            return None
        existing = self._procs.get(reg.name)

        if existing:
            existing.proc.poll()
            if existing.proc.returncode is None:
                return existing

        env = os.environ.copy()
        if reg.kickoff_env:
            env.update(reg.kickoff_env)

        # Use filtered kubeconfig if set (hides chaos engineering namespaces)
        if self._agent_kubeconfig_path:
            env["KUBECONFIG"] = self._agent_kubeconfig_path
            env["SREGYM_BASE_KUBECONFIG"] = self._agent_kubeconfig_path

        command = reg.kickoff_command
        if extra_args:
            command += f" {extra_args}"

        # SREGYM_EXP_ENV is the per-worker scratch directory where agents run.
        # It serves as the agent's working directory (cwd) and is where shared
        # files (instruction.txt, trajectories, summaries) are written. Each
        # parallel worker gets its own exp_env (set by main.py) to avoid conflicts.
        # Cleaned between problems by _clean_exp_env().
        exp_env_dir = os.getenv("SREGYM_EXP_ENV", "exp_env")
        os.makedirs(exp_env_dir, exist_ok=True)

        # Use start_new_session on Unix so we can kill the entire process group
        # (agent may spawn child processes, e.g. Gemini CLI subprocesses)
        start_new_session = sys.platform != "win32"
        proc = subprocess.Popen(
            command,
            shell=True,
            cwd=reg.kickoff_workdir or os.getcwd(),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            start_new_session=start_new_session,
        )
        # Persist PGID so supervisor can kill orphaned agents after worker crashes.
        # With start_new_session=True the child is the session leader, so PGID == PID.
        pgid_file = os.path.join(exp_env_dir, "agent.pgid")
        try:
            with open(pgid_file, "w") as f:
                f.write(str(proc.pid))
        except OSError:
            pass
        ap = AgentProcess(reg.name, proc)
        self._procs[reg.name] = ap
        t = threading.Thread(target=self._pipe_logs, args=(reg.name, proc), daemon=True)
        t.start()
        return ap

    def cleanup_all_agents(self, timeout: int = 5) -> None:
        """Terminate all running agents. Used when worker receives SIGTERM/SIGINT."""
        for name in list(self._procs.keys()):
            self.cleanup_agent(name, timeout=timeout)

    def _pipe_logs(self, name: str, proc: subprocess.Popen):
        if proc.stdout is None:
            return
        for line in proc.stdout:
            try:
                sys.stdout.write(f"{line}")
                sys.stdout.flush()
            except Exception:
                break

    def cleanup_agent(self, agent_name: str, timeout: int = 10) -> None:
        """
        Terminate and cleanup an agent process and its entire process group.
        Ensures no stray agent processes remain when starting the next problem.

        Args:
            agent_name: Name of the agent to cleanup
            timeout: Seconds to wait for graceful termination before force kill
        """
        existing = self._procs.get(agent_name)
        if not existing:
            return

        # Check if already terminated
        existing.proc.poll()
        if existing.proc.returncode is not None:
            # Already terminated, just remove from cache
            del self._procs[agent_name]
            self._clean_exp_env()
            return

        pid = existing.proc.pid
        pgid = None
        if sys.platform != "win32" and pid is not None:
            try:
                pgid = os.getpgid(pid)
            except (ProcessLookupError, OSError):
                pass

        # Terminate entire process group (kills child processes like Gemini CLI subprocesses)
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                existing.proc.terminate()
            try:
                existing.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Force kill if timeout exceeded
                if pgid is not None:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass
                try:
                    existing.proc.kill()
                    existing.proc.wait(timeout=2)
                except (subprocess.TimeoutExpired, ProcessLookupError):
                    pass
        except (ProcessLookupError, OSError):
            pass
        finally:
            # Remove from cache and ensure process is gone
            if agent_name in self._procs:
                del self._procs[agent_name]
            # Remove PGID file before cleaning exp_env
            pgid_file = os.path.join(os.getenv("SREGYM_EXP_ENV", "exp_env"), "agent.pgid")
            try:
                os.remove(pgid_file)
            except OSError:
                pass
            self._clean_exp_env()

    def _clean_exp_env(self):
        """Clean up all files in exp_env directory by deleting and recreating it."""
        exp_env = os.getenv("SREGYM_EXP_ENV", "exp_env")
        try:
            if os.path.exists(exp_env):
                shutil.rmtree(exp_env)
            os.makedirs(exp_env, exist_ok=True)
        except Exception:
            pass
