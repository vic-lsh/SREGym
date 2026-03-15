import json
import logging
import os

from sregym.paths import TARGET_MICROSERVICES


class Application:
    """Base class for all microservice applications."""

    def __init__(self, config_file: str):
        self.config_file = config_file
        self.name = None
        self.namespace = None
        self.helm_deploy = True
        self.helm_configs = {}
        self.k8s_deploy_path = None
        self.logger = logging.getLogger("all.application")
        self.logger.propagate = True
        self.logger.setLevel(logging.DEBUG)

    def load_app_json(self):
        """Load (basic) application metadata into attributes.

        # NOTE: override this method to load additional attributes!
        """
        with open(self.config_file, "r") as file:
            metadata = json.load(file)

        self.name = metadata["Name"]
        self.namespace = metadata["Namespace"]

        if "Helm Config" in metadata:
            self.helm_configs = metadata["Helm Config"]
            chart_path = self.helm_configs.get("chart_path")

            if chart_path and not self.helm_configs.get("remote_chart", False):
                self.helm_configs["chart_path"] = str(TARGET_MICROSERVICES / chart_path)

        if "K8S Deploy Path" in metadata:
            self.k8s_deploy_path = TARGET_MICROSERVICES / metadata["K8S Deploy Path"]

    def get_app_json(self) -> dict:
        """Get application metadata in JSON format.

        Returns:
            dict: application metadata
        """
        with open(self.config_file, "r") as file:
            app_json = json.load(file)
        return app_json

    def get_app_summary(self) -> str:
        """Get a summary of the application metadata in string format.
        NOTE: for human and LLM-readable summaries!

        Returns:
            str: application metadata
        """
        app_json = self.get_app_json()
        app_name = app_json.get("Name", "")
        namespace = app_json.get("Namespace", "")
        desc = app_json.get("Desc", "")
        supported_operations = app_json.get("Supported Operations", [])
        operations_str = "\n".join([f"  - {op}" for op in supported_operations])

        description = f"App Name: {app_name}\nNamespace: {namespace}\nDescription: {desc}\nSupported Operations:\n{operations_str}"

        return description

    def create_namespace(self):
        """Create the namespace for the application if it doesn't exist."""
        result = self.kubectl.exec_command(f"kubectl get namespace {self.namespace}")
        if "notfound" in result.lower():
            self.logger.info(f"Namespace {self.namespace} not found. Creating namespace.")
            create_namespace_command = f"kubectl create namespace {self.namespace}"
            create_result = self.kubectl.exec_command(create_namespace_command)
            self.logger.info(f"Namespace {self.namespace} created successfully: {create_result}")
        else:
            self.logger.info(f"Namespace {self.namespace} already exists.")

    def configure_dockerhub_pull_secret(
        self, patch_all_service_accounts: bool = False, restart_pods: bool = False
    ) -> None:
        """Configure Docker Hub pull credentials in the app namespace when available."""
        enabled = os.getenv("SREGYM_ENABLE_DOCKERHUB_PULL_SECRET", "1").strip().lower()
        if enabled in {"0", "false", "no"}:
            self.logger.info("[DEPLOY] Docker Hub imagePullSecret injection disabled.")
            return

        secret_name = os.getenv("SREGYM_DOCKER_PULL_SECRET_NAME", "regcred")
        docker_user = os.environ.get("DOCKER_USERNAME")
        docker_password = os.environ.get("DOCKER_PASSWORD")

        secret_created = False

        if docker_user and docker_password:
            self.logger.info(f"Creating Docker registry secret '{secret_name}' from env vars...")
            cmd = (
                f"kubectl create secret docker-registry {secret_name} "
                f"--docker-server=https://index.docker.io/v1/ "
                f"--docker-username={docker_user} "
                f"--docker-password={docker_password} "
                f"--docker-email=sregym@example.com "
                f"-n {self.namespace} "
                f"--dry-run=client -o yaml | kubectl apply -f -"
            )
            self.kubectl.exec_command(cmd)
            secret_created = True
        else:
            self.logger.warning(
                "DOCKER_USERNAME and DOCKER_PASSWORD env vars not found. Skipping Docker Hub secret creation."
            )
            # Fallback to config.json removed as per user instruction to strictly use env vars
            return

        if secret_created:
            service_accounts = ["default"]
            if patch_all_service_accounts:
                escaped_ns = self.namespace.replace("'", "'\"'\"'")
                service_accounts_output = self.kubectl.exec_command(
                    f"kubectl -n '{escaped_ns}' get sa -o jsonpath='{{.items[*].metadata.name}}'"
                ).strip()
                if service_accounts_output:
                    service_accounts = service_accounts_output.split()

            for sa_name in service_accounts:
                escaped_sa = sa_name.replace("'", "'\"'\"'")
                escaped_ns = self.namespace.replace("'", "'\"'\"'")
                self.kubectl.exec_command(
                    f"kubectl -n '{escaped_ns}' patch sa '{escaped_sa}' --type=merge -p "
                    f'\'{{"imagePullSecrets":[{{"name":"{secret_name}"}}]}}\''
                )

            if restart_pods:
                escaped_ns = self.namespace.replace("'", "'\"'\"'")
                self.kubectl.exec_command(f"kubectl -n '{escaped_ns}' delete pod --all --ignore-not-found")

            self.logger.info(
                f"[DEPLOY] Configured app imagePullSecrets in namespace '{self.namespace}' using '{secret_name}'."
            )

    def cleanup(self):
        """Delete the entire namespace for the application."""
        self.kubectl.delete_namespace(self.namespace)
