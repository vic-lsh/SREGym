# Source-installed reservation cleanup controller

This opt-in variant keeps the old FinalizerDeadlock implementation and oracle
unchanged. Set `SREGYM_HOTEL_CLEANUP_CONTROLLER=1` **before creating a fresh hotel
application workspace**. Workspace preparation activates the optional source
component and commits all five objects in the initial application snapshot at
`kubernetes/cleanup-controller.yaml`. Seeded workspaces remain unchanged and must
already contain the component. The source controller image is
`python:3.12-alpine`; make it available in the lane before setup.

Use `finalizer_deadlock_controller_preinstalled_hotel_reservation` alone or
`composite_slot11_preinstalled_hotel_reservation`. The named composite replaces
only FinalizerDeadlock in slot 11: the profile targetPort remains 8888 and the
mongodb-geo user remains unregistered. It uses the original colocated-composite
lifecycle and the same per-component mitigation oracles; the original random
slot ID and catalog stay unchanged. Use this distinct ID when comparing results.

The new fault validates that the source-installed controller is Ready, the
service account/script/binding exist, and the RBAC exactly matches source.
Injection creates only the injected runtime token, removes ConfigMap write
verbs from the ClusterRole, requests token deletion and requires a fresh real
403 log. No benchmark host reconciliation thread is started. Source objects
never gain `platform.io/injected`. Recovery restores only rules and removes the
runtime token; it retains every source-installed support object.

The old `drop_subresource_rule` mode is deliberately excluded: Kubernetes
patches ConfigMap finalizers through the ConfigMap API, which has no finalizers
subresource. The original benchmark simulates that mode on its host thread.
The new variant's `remove_write_verbs` mode denies the controller's real patch.

Offline tests run the actual application controller function against a stateful
fake API, inject through the variant, restore accepted source rules with a
scripted responder and evaluate the unchanged oracle. Deleting the controller
fails the oracle and leaves its source-declared identity missing. No model,
cluster, Docker operation or credential read is needed by these tests.
