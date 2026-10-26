"""A warm lane's full recovery removes every cluster-scoped object its injection created."""

from dataclasses import dataclass, field

import pytest

from sregym.fastloop.created_objects import CLUSTER_KINDS, ClusterObject, remove_created
from sregym.fastloop.worker import FastloopWorker

PRE_EXISTING = frozenset(
    {
        ClusterObject("Namespace", "hotel-reservation"),
        ClusterObject("Namespace", "policy-system"),
        ClusterObject("ClusterRole", "admin"),
    }
)
# What `helm upgrade --install ingress-nginx ... --create-namespace` leaves on the cluster.
INGRESS_NGINX = frozenset(
    {
        ClusterObject("Namespace", "ingress-nginx"),
        ClusterObject("ClusterRole", "ingress-nginx"),
        ClusterObject("ClusterRoleBinding", "ingress-nginx"),
        ClusterObject("IngressClass", "nginx"),
        ClusterObject("ValidatingWebhookConfiguration", "ingress-nginx-admission"),
    }
)


@dataclass
class FakeInventory:
    objects: set[ClusterObject] = field(default_factory=lambda: set(PRE_EXISTING))
    deleted: list[ClusterObject] = field(default_factory=list)
    # Namespaces stay listed (Terminating) for this many lists after their delete.
    terminating_lists: int = 0
    unlistable: bool = False
    _terminating: dict[ClusterObject, int] = field(default_factory=dict)

    def list_objects(self, namespaces=frozenset()):
        if self.unlistable:
            raise RuntimeError("synthetic list failure")
        for obj in list(self._terminating):
            self._terminating[obj] -= 1
            if self._terminating[obj] < 0:
                del self._terminating[obj]
                self.objects.discard(obj)
        return frozenset(obj for obj in self.objects if not obj.namespace or obj.namespace in namespaces)

    def delete(self, obj):
        self.deleted.append(obj)
        if obj.kind == "Namespace" and self.terminating_lists:
            self._terminating.setdefault(obj, self.terminating_lists)
        else:
            self.objects.discard(obj)


@dataclass
class App:
    namespace: str = "hotel-reservation"
    name: str = "Hotel Reservation"


@dataclass
class InstallsControllerProblem:
    """Like IngressMisroute: inject installs a controller; recover only reverts the fault."""

    inventory: FakeInventory
    app: App = field(default_factory=App)
    events: list[str] = field(default_factory=list)
    breaks_inventory: bool = False

    @property
    def namespace(self):
        return self.app.namespace

    def inject_fault(self):
        self.events.append("inject")
        self.inventory.objects |= INGRESS_NGINX
        self.inventory.unlistable = self.breaks_inventory

    def recover_fault(self):
        self.events.append("recover")


def worker(inventory, problems=None):
    made = problems if problems is not None else []

    def factory(_):
        made.append(InstallsControllerProblem(inventory))
        return made[-1]

    w = FastloopWorker(
        problem_factory=factory,
        deployer=lambda *_: None,
        health_probe=lambda _: {},
        cluster_factory=lambda *_: ("fake", "fake"),
        cluster_inventory=inventory,
    )
    w.created_objects_timeout_s = 5.0
    w.created_objects_poll_s = 0.0
    return w, made


def test_full_recovery_removes_exactly_the_objects_injection_created():
    inventory = FakeInventory()
    w, _ = worker(inventory)
    w.handle({"op": "inject", "problem_id": "ingress_misroute"})
    assert inventory.objects >= INGRESS_NGINX

    result = w.handle({"op": "recover", "mode": "full"})

    assert result["success"] is True, result
    assert inventory.objects == PRE_EXISTING
    assert set(inventory.deleted) == INGRESS_NGINX
    # Admission webhooks go first and namespaces last.
    assert inventory.deleted[0].kind == "ValidatingWebhookConfiguration"
    assert inventory.deleted[-1] == ClusterObject("Namespace", "ingress-nginx")
    # The next row is admitted on an unchanged inventory.
    w.handle({"op": "inject", "problem_id": "ingress_misroute"})


def test_objects_that_existed_before_injection_are_never_deleted():
    inventory = FakeInventory()
    inventory.objects |= INGRESS_NGINX  # e.g. a controller the lane already had
    w, _ = worker(inventory)
    w.handle({"op": "inject", "problem_id": "ingress_misroute"})

    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    assert inventory.deleted == []
    assert inventory.objects == PRE_EXISTING | INGRESS_NGINX


def test_recovery_waits_for_a_terminating_namespace_to_disappear():
    inventory = FakeInventory(terminating_lists=3)
    w, _ = worker(inventory)
    w.handle({"op": "inject", "problem_id": "ingress_misroute"})

    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    assert ClusterObject("Namespace", "ingress-nginx") not in inventory.objects


def test_leftover_created_object_fails_recovery_and_blocks_the_next_injection():
    inventory = FakeInventory(terminating_lists=10_000)
    w, made = worker(inventory)
    w.created_objects_timeout_s = 0.0
    w.handle({"op": "inject", "problem_id": "ingress_misroute"})

    result = w.handle({"op": "recover", "mode": "full"})

    assert result["success"] is False
    assert "Namespace/ingress-nginx" in result["errors"][0]["error"]
    with pytest.raises(RuntimeError, match="recovery failed"):
        w.handle({"op": "inject", "problem_id": "next"})
    # A retry finishes the removal once the namespace is gone, without re-running recover_fault.
    inventory.terminating_lists = 0
    inventory._terminating.clear()
    inventory.objects.discard(ClusterObject("Namespace", "ingress-nginx"))
    assert w.handle({"op": "recover", "mode": "full"})["success"] is True
    assert made[0].events == ["inject", "recover"]
    w.handle({"op": "inject", "problem_id": "next"})


def test_unlistable_inventory_refuses_injection_before_any_write():
    inventory = FakeInventory(unlistable=True)
    w, made = worker(inventory)

    with pytest.raises(RuntimeError, match="cluster-scoped inventory"):
        w.handle({"op": "inject", "problem_id": "ingress_misroute"})
    assert made == [] or made[0].events == []


def test_unlistable_inventory_after_injection_marks_it_invalid_and_recovery_fails_closed():
    inventory = FakeInventory()
    w, made = worker(inventory)
    w._problem_factory = lambda _: made.append(InstallsControllerProblem(inventory, breaks_inventory=True)) or made[-1]

    result = w.handle({"op": "inject", "problem_id": "ingress_misroute"})

    assert result["injection"]["valid"] is False
    assert "cluster-scoped inventory" in result["injection"]["details"]["created_objects_error"]
    inventory.unlistable = False
    recovered = w.handle({"op": "recover", "mode": "full"})
    assert recovered["success"] is False
    assert "unknown" in recovered["errors"][0]["error"]


def test_remove_created_orders_every_supported_kind():
    inventory = FakeInventory()
    created = {ClusterObject(kind, f"x-{kind.lower()}") for kind in CLUSTER_KINDS}
    inventory.objects |= created

    remaining, error = remove_created(inventory, created, timeout_s=1.0, sleep=lambda _: None)

    assert remaining == frozenset() and error == ""
    assert [obj.kind for obj in inventory.deleted] == list(CLUSTER_KINDS)


def test_cluster_object_rejects_unsupported_kinds():
    with pytest.raises(ValueError, match="unsupported"):
        ClusterObject("Node", "worker-1")
