import os
import socket
import subprocess
import time
from pathlib import Path


class Jaeger:
    def __init__(self, namespace="observe"):
        self.namespace = namespace
        base_dir = Path(__file__).parent
        self.config_file = base_dir / "jaeger.yaml"
        self.port = self._pick_free_port()
        self.port_forward_process = None
        os.environ["JAEGER_BASE_URL"] = f"http://localhost:{self.port}"

    def _pick_free_port(self) -> int:
        """Pick a free local TCP port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def run_cmd(self, cmd: str) -> str:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Command failed: {cmd}\nError: {result.stderr}")
        return result.stdout.strip()

    def deploy(self):
        """Deploy Jaeger with TiDB as the storage backend."""
        # Ensure namespace exists before deploying
        self.run_cmd(f"kubectl create ns {self.namespace} --dry-run=client -o yaml | kubectl apply -f -")
        self.run_cmd(f"kubectl apply -f {self.config_file} -n {self.namespace}")
        self.wait_for_service("jaeger-out", timeout=120)
        self.start_port_forward()
        print("Jaeger deployed successfully.")

    def is_port_in_use(self, port: int) -> bool:
        """Check if a local TCP port is already bound."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    def wait_for_service(self, service: str, timeout: int = 60):
        """Wait until the Jaeger service exists in Kubernetes."""
        print(f"[debug] waiting for service {service} in ns={self.namespace}")
        t0 = time.time()
        while time.time() - t0 < timeout:
            result = subprocess.run(
                f"kubectl -n {self.namespace} get svc {service}",
                shell=True,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                print(f"[debug] found service {service}")
                return
            time.sleep(3)
        raise RuntimeError(f"Service {service} not found within {timeout}s")

    def start_port_forward(self):
        """Starts port-forwarding to access Prometheus."""
        print("Start port-forwarding for Prometheus.")
        if self.port_forward_process and self.port_forward_process.poll() is None:
            print("Port-forwarding already active.")
            return

        for attempt in range(3):
            if self.is_port_in_use(self.port):
                print(f"Port {self.port} is already in use. Picking a new one...")
                self.port = self._pick_free_port()
                os.environ["JAEGER_BASE_URL"] = f"http://localhost:{self.port}"

            command = f"kubectl port-forward svc/jaeger-out {self.port}:16686 -n {self.namespace}"
            self.port_forward_process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            os.environ["JAEGER_PORT"] = str(self.port)
            time.sleep(3)  # Wait a bit for the port-forward to establish

            if self.port_forward_process.poll() is None:
                print(f"Port forwarding established at {self.port}.")
                os.environ["JAEGER_PORT"] = str(self.port)
                break
            else:
                print("Port forwarding failed. Retrying...")
                # Kill process if it failed but didn't exit cleanly?
                # poll() is None means it is running. Here it is NOT None, so it exited.
        else:
            print("Failed to establish port forwarding after multiple attempts.")

    def stop_port_forward(self):
        """Stops the kubectl port-forward command and cleans up resources."""
        if self.port_forward_process:
            self.port_forward_process.terminate()
            try:
                self.port_forward_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("Port-forward process did not terminate in time, killing...")
                self.port_forward_process.kill()

            if self.port_forward_process.stdout:
                self.port_forward_process.stdout.close()
            if self.port_forward_process.stderr:
                self.port_forward_process.stderr.close()

            print("Port forwarding stopped.")


if __name__ == "__main__":
    jaeger = Jaeger()
