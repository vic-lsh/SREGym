from sregym.conductor.oracles.base import Oracle

# MongoDB database each hotel-reservation mongo service's admin user reads/writes.
_MONGO_TARGET_DB = {"mongodb-rate": "rate-db", "mongodb-geo": "geo-db"}


class MongoAuthMitigationOracle(Oracle):
    """Data-plane oracle for MongoDB auth faults (revoke_auth, storage_user_unregistered).

    The readiness-based MitigationOracle cannot detect these faults: revoking a
    role or dropping the admin user leaves the mongo pod (and dependent service
    pods) Running/Ready — they merely fail DB operations with authorization
    errors. This oracle probes the actual data plane: it execs into the target
    mongo pod and attempts an authenticated read on the service's database as the
    ``admin`` user.

      * revoke_auth: readWrite (and thus read) on the target DB is revoked, so
        the read is "not authorized" -> fault active.
      * storage_user_unregistered: the admin user is dropped, so authentication
        itself fails -> fault active.

    In both cases the probe succeeds only once mitigation restores the role/user,
    so it grades the mitigation correctly.
    """

    importance = 1.0

    def evaluate(self) -> dict:
        print("== MongoDB Auth Mitigation Check ==")

        kubectl = self.problem.kubectl
        namespace = self.problem.namespace
        faulty_service = self.problem.faulty_service
        target_db = _MONGO_TARGET_DB.get(faulty_service)
        if target_db is None:
            print(f"❌ Unknown mongo service '{faulty_service}' — cannot probe auth")
            return {"success": False}

        pods = kubectl.list_pods(namespace)
        mongo_pods = [p.metadata.name for p in pods.items if faulty_service in p.metadata.name]
        if not mongo_pods:
            print(f"❌ No pod found for mongo service '{faulty_service}'")
            return {"success": False}

        pod = mongo_pods[0]
        # Read-only probe: requires the read privilege on the target DB. Prints a
        # sentinel only if the authenticated read succeeds; if auth fails or the
        # role is missing, the eval throws before printing and exits non-zero.
        js = "db.probe.find().limit(1).toArray(); print('PROBE_OK');"
        cmd = (
            f"kubectl exec {pod} -n {namespace} -- "
            f"mongo {target_db} -u admin -p admin --authenticationDatabase admin "
            f'--eval "{js}"'
        )
        output = kubectl.exec_command(cmd)

        if "PROBE_OK" in output:
            print(f"[✅] admin can read {target_db} in {pod} — auth restored")
            return {"success": True}

        print(f"[❌] admin cannot read {target_db} in {pod} — auth fault still active")
        return {"success": False}
