import contextlib
import fcntl
import hashlib
import json
import logging
import os
import platform
import shutil
import subprocess
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from clients.harness.claude_auth import container_auth

logger = logging.getLogger("all.sregym.container_runner")

LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BUILD_SCRIPT = REPO_ROOT / "docker" / "agents" / "build.sh"
CHECK_SCRIPT = REPO_ROOT / "docker" / "agents" / "check_image.sh"

#: The agent image is tagged ``<repo>:<build context hash>``: an image under that tag was built from
#: exactly this checkout's agent code, and a build from other code (another worktree, an older
#: branch) gets another tag instead of replacing it. The hash is also stored in this label.
AGENT_IMAGE_REPO = "sregym-agent-slim"
CONTEXT_LABEL = "sregym.agent-context"

# Images (by ID) that passed check_image.sh in this process.
_VERIFIED_IMAGE_IDS: set[str] = set()


def tree_hash(root: Path) -> str:
    """Hash of the paths, executable bits and contents of the files under *root*."""
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        data = path.read_bytes()
        rel = path.relative_to(root).as_posix().encode()
        mode = b"x" if os.access(path, os.X_OK) else b"-"
        digest.update(b"%d:%s:%s:%d:" % (len(rel), rel, mode, len(data)) + data)
    return digest.hexdigest()[:16]


def agent_context_hash() -> str:
    """Hash of the agent image's build context as ``build.sh --context`` assembles it now."""
    with tempfile.TemporaryDirectory(prefix="sregym-agent-context-") as tmp:
        ctx = Path(tmp) / "ctx"
        subprocess.run(["bash", str(BUILD_SCRIPT), "--context", str(ctx)], check=True, capture_output=True)
        return tree_hash(ctx)


@contextlib.contextmanager
def _image_build_lock():
    """Serialize image checks and builds across this user's SREGym processes (parallel workers)."""
    lock_dir = Path.home() / ".cache" / "sregym"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with open(lock_dir / "agent-image.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _docker_uses_separate_host() -> bool:
    """Return whether Docker runs outside the host's network namespace."""
    return platform.system() == "Darwin" or "microsoft" in platform.release().lower()


def _replace_loopback_host(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname not in LOOPBACK_HOSTS:
        return url

    userinfo, separator, _ = parsed.netloc.rpartition("@")
    netloc = f"{userinfo}{separator}host.docker.internal"
    if parsed.port is not None:
        netloc += f":{parsed.port}"
    return urlunsplit(parsed._replace(netloc=netloc))


@dataclass
class ExecInput:
    command: str
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    timeout: int | None = None  # seconds, None = no timeout
    label: str = ""
    container_name: str = ""


@dataclass
class ContainerConfig:
    image: str | None = None  # None: f"{AGENT_IMAGE_REPO}:{agent_context_hash()}"
    network_mode: str = "host"
    kubeconfig_path: Path | None = None
    workspace_path: Path | None = None  # bind-mounted to /workspace for agent output
    logs_path: Path | None = None
    env_vars: dict = field(default_factory=dict)
    cpus: float = 4.0
    memory: str = "8g"


class ContainerRunner:
    # Env vars forwarded from host to agent containers.
    # Sourced from litellm provider source code (llms/<provider>/).
    API_KEY_VARS = [
        # OpenAI
        "OPENAI_API_KEY",
        "OPENAI_API_BASE",
        "OPENAI_BASE_URL",
        # DeepSeek
        "DEEPSEEK_API_KEY",
        "DEEPSEEK_API_BASE",
        # Anthropic
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_API_BASE",
        # Gemini / Google
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "GEMINI_API_BASE",
        # Azure OpenAI
        "AZURE_API_KEY",
        "AZURE_OPENAI_API_KEY",
        "AZURE_API_BASE",
        "AZURE_API_VERSION",
        "AZURE_AD_TOKEN",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_TENANT_ID",
        "AZURE_USERNAME",
        "AZURE_PASSWORD",
        "AZURE_CERTIFICATE_PATH",
        "AZURE_CERTIFICATE_PASSWORD",
        "AZURE_CREDENTIAL",
        "AZURE_SCOPE",
        "AZURE_AUTHORITY_HOST",
        "AZURE_FEDERATED_TOKEN_FILE",
        # AWS Bedrock
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION_NAME",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_SESSION_NAME",
        "AWS_PROFILE",
        "AWS_PROFILE_NAME",
        "AWS_ROLE_NAME",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_STS_ENDPOINT",
        "AWS_EXTERNAL_ID",
        "AWS_BEDROCK_RUNTIME_ENDPOINT",
        "AWS_BEARER_TOKEN_BEDROCK",
        # WatsonX / IBM
        "WATSONX_API_KEY",
        "WATSONX_APIKEY",
        "WATSONX_API_BASE",
        "WATSONX_URL",
        "WATSONX_TOKEN",
        "WATSONX_PROJECT_ID",
        "WATSONX_REGION",
        "WATSONX_SPACE_ID",
        "WATSONX_DEPLOYMENT_SPACE_ID",
        "WATSONX_IAM_URL",
        "WATSONX_ZENAPIKEY",
        "WX_API_KEY",
        "WX_PROJECT_ID",
        "WX_URL",
        "WX_REGION",
        "WX_SPACE_ID",
        "WML_URL",
        # Vertex AI
        "VERTEXAI_PROJECT",
        "VERTEXAI_LOCATION",
        "VERTEX_LOCATION",
        "VERTEXAI_CREDENTIALS",
        "GOOGLE_APPLICATION_CREDENTIALS",
        # Moonshot
        "MOONSHOT_API_KEY",
        "MOONSHOT_API_BASE",
        # GLM
        "GLM_API_KEY",
        "ZAI_API_KEY",
        "ZHIPU_API_KEY",
        # Claude Code
        "CLAUDE_CODE_OAUTH_TOKEN",
        # GitHub Copilot CLI
        "COPILOT_GITHUB_TOKEN",
        "COPILOT_PROVIDER_BASE_URL",
        "COPILOT_PROVIDER_API_KEY",
        "COPILOT_PROVIDER_TYPE",
        # SREGym internal
        "AGENT_MODEL_ID",
        "AGENT_REASONING_EFFORT",
        "AGENT_MAX_TURNS",
        "SREGYM_AGENT_PROMPT_APPENDIX",
        "AGENT_API_BASE",
        "AGENT_API_KEY",
        # JUDGE_* stay on the host: grading runs in the conductor, not the agent.
        # Config vars
        "API_HOSTNAME",
        "API_PORT",
        "MCP_SERVER_PORT",
        "MCP_SERVER_URL",
        "EXPOSE_SERVER",
        "SESSION_CACHE_SIZE",
        "SESSION_TTL",
        "LLM_QUERY_MAX_RETRIES",
        "LLM_QUERY_INIT_RETRY_DELAY",
        "WAIT_FOR_POD_READY_TIMEOUT",
    ]

    def __init__(self, config: ContainerConfig | None = None):
        self.config = config or ContainerConfig()
        self._credential_tmps: list[str] = []
        self._context_hash: str | None = None
        self._image_id: str | None = None  # set once the image passed the isolation check

    @property
    def context_hash(self) -> str:
        if self._context_hash is None:
            self._context_hash = agent_context_hash()
        return self._context_hash

    @property
    def image(self) -> str:
        if self.config.image is None:
            self.config.image = f"{AGENT_IMAGE_REPO}:{self.context_hash}"
        return self.config.image

    def _mount_codex_credentials(self, args: list[str]) -> None:
        """Mount Codex auth into a throwaway tempdir

        Copies only auth.json + config.toml and never sessions/logs/telemetry
        so each container gets isolated state that dies with it.
        """
        codex_dir = Path.home() / ".codex"
        if not codex_dir.is_dir():
            return

        tmp = tempfile.mkdtemp(prefix="sregym-codex-")
        auth_src = codex_dir / "auth.json"
        # Reject symlinks + check readability (files may be root-owned from container writes).
        if auth_src.is_symlink() or not auth_src.is_file() or not os.access(auth_src, os.R_OK):
            shutil.rmtree(tmp, ignore_errors=True)
            return

        shutil.copy2(auth_src, Path(tmp) / "auth.json")

        args.extend(["-v", f"{tmp}:/root/.codex"])
        self._credential_tmps.append(tmp)

    def cleanup_credential_tmps(self) -> None:
        """Remove throwaway credential directories."""
        for tmp in self._credential_tmps:
            shutil.rmtree(tmp, ignore_errors=True)
        self._credential_tmps = []

    def _build_env_flags(self, extra_env: dict[str, str] | None = None) -> list[str]:
        flags = []
        env_vars = dict(self.config.env_vars)

        # Forward API keys from host (skip empty values to avoid overriding
        # other auth mechanisms like OAuth subscription tokens)
        for var in self.API_KEY_VARS:
            if var in os.environ and var not in env_vars and os.environ[var]:
                env_vars[var] = os.environ[var]

        # Claude provider auth (API key, OAuth or Vertex), with the Claude settings fallback.
        auth, _ = container_auth(os.environ, Path.home())
        env_vars.update({k: v for k, v in auth.items() if k not in self.config.env_vars})

        if extra_env:
            env_vars.update(extra_env)

        # Docker Desktop containers cannot reach host loopback directly.
        if _docker_uses_separate_host() and (api_base := env_vars.get("AGENT_API_BASE")):
            env_vars["AGENT_API_BASE"] = _replace_loopback_host(api_base)

        # Agent containers use Docker's host alias to reach SREGym services
        # running on the host, including the MCP port-forward.
        if self.config.network_mode == "host":
            env_vars["API_HOSTNAME"] = "host.docker.internal"
            mcp_port = env_vars.get("MCP_SERVER_PORT", os.environ.get("MCP_SERVER_PORT", "9954"))
            env_vars["MCP_SERVER_URL"] = f"http://host.docker.internal:{mcp_port}"

        for key, value in env_vars.items():
            flags.extend(["-e", f"{key}={value}"])
        return flags

    def _build_base_docker_args(self) -> list[str]:
        args = [
            "docker",
            "run",
            "--rm",
            f"--cpus={self.config.cpus}",
            f"--memory={self.config.memory}",
        ]

        # Configure networking based on the network mode
        if self.config.network_mode == "host":
            if platform.system() == "Darwin":
                # macOS: Don't use --network host (it's ignored), rely on host.docker.internal
                args.append("--add-host=host.docker.internal:host-gateway")
            else:
                # Linux: --network=host is unreliable in some configurations.
                # --add-host injects the host IP directly into /etc/hosts.
                args.append("--network=host")
                args.append("--add-host=host.docker.internal:host-gateway")
        else:
            args.append(f"--network={self.config.network_mode}")

        # The only kubeconfig in the container is the filtering proxy's. Grading
        # runs in the conductor on the host, so nothing in here needs the real one.
        if self.config.kubeconfig_path and self.config.kubeconfig_path.exists():
            args.extend(["-v", f"{self.config.kubeconfig_path.resolve()}:/root/.kube/config:ro"])
            args.extend(["-e", "KUBECONFIG=/root/.kube/config"])

        # Mount AWS credentials directory (read-only) for Bedrock and other AWS services
        aws_dir = Path.home() / ".aws"
        if aws_dir.is_dir():
            args.extend(["-v", f"{aws_dir.resolve()}:/root/.aws:ro"])

        # Google credentials for Claude on Vertex: only that one file, read-only.
        for host_path, container_path in container_auth(os.environ, Path.home())[1]:
            args.extend(["-v", f"{host_path}:{container_path}:ro"])

        self._mount_codex_credentials(args)

        # Mount workspace directory for agent output (logs, results, trajectories)
        if self.config.workspace_path:
            self.config.workspace_path.mkdir(parents=True, exist_ok=True)
            args.extend(["-v", f"{self.config.workspace_path.resolve()}:/workspace"])

        # Mount logs directory (for composite command tee output)
        if self.config.logs_path:
            self.config.logs_path.mkdir(parents=True, exist_ok=True)
            args.extend(["-v", f"{self.config.logs_path.resolve()}:/logs"])

        return args

    def build_docker_command(self, exec_input: ExecInput) -> list[str]:
        if self._image_id is None:
            raise RuntimeError("agent image not checked: call ensure_image_exists() before running agents")
        cmd = self._build_base_docker_args()
        suffix = uuid.uuid4().hex[:8]
        if exec_input.label:
            container_name = f"sregym-{exec_input.label}-{suffix}"
            cmd.extend(["--name", container_name])
            exec_input.container_name = container_name
        cmd.extend(self._build_env_flags(exec_input.env))
        # By ID: the image that passed the check, even if the tag moves meanwhile.
        cmd.append(self._image_id)
        cmd.append(exec_input.command)
        return cmd

    def build_composite_command(
        self,
        install_script: str | None,
        agent_version: str | None,
        driver_command: str,
    ) -> str:
        parts = []

        if install_script:
            version_env = f'AGENT_VERSION="{agent_version}" ' if agent_version else ""
            parts.append(
                f"{version_env}/opt/sregym/install-scripts/{install_script} 2>&1 "
                f"| tee /logs/install.log; INSTALL_RC=${{PIPESTATUS[0]}}; "
                f'echo "$INSTALL_RC" > /logs/install.rc; '
                f'[ "$INSTALL_RC" -eq 0 ] || exit "$INSTALL_RC"'
            )

        parts.append(
            f"{driver_command} 2>&1 "
            f"| tee /logs/driver.log; DRIVER_RC=${{PIPESTATUS[0]}}; "
            f'echo "$DRIVER_RC" > /logs/driver.rc; '
            f'exit "$DRIVER_RC"'
        )

        return " && ".join(parts)

    def run_sync(self, exec_input: ExecInput) -> subprocess.CompletedProcess:
        """Run a short-lived command in a container and wait for it to finish.

        Unlike run_async, this blocks until the container exits and returns the
        CompletedProcess with captured stdout/stderr.  Useful for pre-flight
        checks (e.g. model validation) that must complete before the main
        agent container is launched.
        """
        cmd = self.build_docker_command(exec_input)

        try:
            return subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=exec_input.timeout,
            )
        except (KeyboardInterrupt, subprocess.TimeoutExpired):
            if exec_input.container_name:
                ContainerRunner.stop_container(exec_input.container_name, timeout=5)
            raise
        finally:
            self.cleanup_credential_tmps()

    def run_async(self, exec_input: ExecInput) -> subprocess.Popen:
        """Start an agent in a container asynchronously. Returns Popen handle."""
        cmd = self.build_docker_command(exec_input)
        logger.info(f"Starting containerized agent [{exec_input.label}]: {exec_input.command[:80]}...")

        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def ensure_image_exists(self, force_build: bool = False) -> None:
        """Make the agent image ready to run, or raise.

        Builds it when missing, when its context label differs from this checkout's agent code, or
        when *force_build*. Then runs ``check_image.sh`` against it (once per image per process):
        an image holding answer-key material or failing to import an agent driver is never run.
        """
        image = self.image
        with _image_build_lock():
            inspected = self._inspect_image(image)
            labels = (inspected or {}).get("Config", {}).get("Labels") or {}
            if force_build or labels.get(CONTEXT_LABEL) != self.context_hash:
                reason = "forced" if force_build else ("missing" if inspected is None else "stale")
                logger.info(f"🐳 Container image '{image}' {reason}. Building...")
                self.build_image()
                inspected = self._inspect_image(image)
                if inspected is None:
                    raise RuntimeError(f"Built container image '{image}' but cannot inspect it.")
            image_id = inspected["Id"]
            if image_id not in _VERIFIED_IMAGE_IDS:
                self._check_image(image, image_id)
                _VERIFIED_IMAGE_IDS.add(image_id)
        self._image_id = image_id

    @staticmethod
    def _inspect_image(image: str) -> dict | None:
        result = subprocess.run(["docker", "image", "inspect", image], capture_output=True, text=True)
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)[0]

    @staticmethod
    def _check_image(image: str, image_id: str) -> None:
        logger.info(f"🔍 Checking container image '{image}' ({image_id[:19]}) for isolation...")
        result = subprocess.run(["bash", str(CHECK_SCRIPT), image_id], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container image '{image}' ({image_id}) failed the isolation check; refusing to run agents "
                f"from it. Rebuild with --force-build.\n{result.stdout}{result.stderr}"
            )

    def build_image(self) -> None:
        """Build (or rebuild) the container image using docker/agents/build.sh."""
        image = self.image
        logger.info(f"🐳 Building container image '{image}'...")

        if not BUILD_SCRIPT.exists():
            raise FileNotFoundError(
                f"Build script not found at {BUILD_SCRIPT}. Cannot auto-build container image '{image}'."
            )

        result = subprocess.run(
            ["bash", str(BUILD_SCRIPT)],
            cwd=str(REPO_ROOT),
            env={**os.environ, "SREGYM_AGENT_IMAGE": image, "SREGYM_AGENT_CONTEXT_HASH": self.context_hash},
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build container image '{image}'. Check the build output above for errors.")
        logger.info(f"✅ Container image '{image}' built successfully.")

    @staticmethod
    def stop_container(container_name: str, timeout: int = 10) -> None:
        """Stop a running container by name. Used for cleanup."""
        try:
            subprocess.run(
                ["docker", "stop", "-t", str(timeout), container_name],
                capture_output=True,
                timeout=timeout + 5,
            )
        except Exception:
            # Force remove if stop fails
            try:
                subprocess.run(
                    ["docker", "rm", "-f", container_name],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                # Force remove if stop fails
                with contextlib.suppress(Exception):
                    subprocess.run(
                        ["docker", "rm", "-f", container_name],
                        capture_output=True,
                        timeout=5,
                    )
