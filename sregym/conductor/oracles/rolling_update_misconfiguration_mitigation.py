import json

from sregym.conductor.oracles.base import Oracle


class RollingUpdateMitigationOracle(Oracle):
    def __init__(self, problem, deployment_name: str):
        super().__init__(problem)
        self.deployment_name = deployment_name
        self.namespace = problem.namespace
        self.kubectl = problem.kubectl

    def evaluate(self) -> dict:
        print("== Rolling Update Mitigation Evaluation ==")

        # IMPORTANT: this oracle only OBSERVES the deployment as the agent left it.
        # It must not mutate the deployment (the previous version replaced the
        # hanging initContainer with `sleep 15` and re-rolled, which self-healed
        # the fault and made the oracle pass vacuously for every profile).
        #
        # A rollout is mitigated iff it has fully completed at the current spec —
        # the same conditions `kubectl rollout status` waits on:
        #   observedGeneration >= metadata.generation  (controller saw the spec)
        #   updatedReplicas    == spec.replicas         (all pods on new template)
        #   status.replicas    == updatedReplicas       (no old-template pods left)
        #   availableReplicas  == spec.replicas         (all pods Available)
        #
        # This FAILS an unmitigated drain_all (availableReplicas < desired) AND an
        # unmitigated stuck_canary (a lingering old pod + stuck canary means
        # replicas != updatedReplicas), and only PASSES once the agent's fix lets
        # the rollout finish.
        try:
            deploy_json = ""
            for _retry in range(3):
                deploy_json = self.kubectl.exec_command(
                    f"kubectl get deployment {self.deployment_name} -n {self.namespace} -o json"
                )
                if deploy_json and deploy_json.strip():
                    break
                import time

                time.sleep(5)
            if not deploy_json or not deploy_json.strip():
                print("⚠️ kubectl returned empty output after retries, namespace may be terminating")
                return {"success": False}
            deploy = json.loads(deploy_json)

            spec_replicas = deploy.get("spec", {}).get("replicas", 1)
            generation = deploy.get("metadata", {}).get("generation")
            status = deploy.get("status", {})
            observed = status.get("observedGeneration")
            updated = status.get("updatedReplicas", 0) or 0
            total = status.get("replicas", 0) or 0
            available = status.get("availableReplicas", 0) or 0

            complete = (
                observed is not None
                and generation is not None
                and observed >= generation
                and updated == spec_replicas
                and total == updated
                and available == spec_replicas
            )

            # The injected `hang-init` init container must be gone. Without this
            # check the `slow_timeout` mode (init runs `sleep 300` then succeeds)
            # self-completes the rollout after 5 minutes with the faulty spec
            # still in place, so the oracle would pass vacuously even though the
            # agent did nothing. A genuine remediation removes the init container.
            init_containers = deploy.get("spec", {}).get("template", {}).get("spec", {}).get("initContainers") or []
            hang_init_present = any(c.get("name") == "hang-init" for c in init_containers)

            print(
                f"desired={spec_replicas} updated={updated} total={total} "
                f"available={available} observedGen={observed} gen={generation} "
                f"hang_init_present={hang_init_present}"
            )
            if complete and not hang_init_present:
                print(
                    "✅ Mitigation successful: rollout is complete, available, and the injected init container is gone"
                )
                return {"success": True}
            if hang_init_present:
                print("❌ Mitigation failed: injected `hang-init` init container is still present")
            else:
                print("❌ Mitigation failed: rollout has not completed / not fully available")
            return {"success": False}

        except Exception as e:
            print(f"❌ Error during evaluation: {str(e)}")
            return {"success": False}
