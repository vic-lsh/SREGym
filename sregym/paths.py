import os
from pathlib import Path

HOME_DIR = Path(os.path.expanduser("~"))
BASE_DIR = Path(__file__).resolve().parent
BASE_PARENT_DIR = Path(__file__).resolve().parent.parent

# Targe microservice and its utilities directories
TARGET_MICROSERVICES = BASE_PARENT_DIR / "SREGym-applications"

# Cache directories
CACHE_DIR = HOME_DIR / "cache_dir"
LLM_CACHE_FILE = CACHE_DIR / "llm_cache.json"

# Cluster baseline state snapshot (captured from a fresh cluster)
CLUSTER_BASELINE_STATE_FILE = CACHE_DIR / "cluster_baseline_state.json"


#: Root of the fault injectors' backup files (original manifests restored on recovery).
FAULT_SCRATCH_ROOT = Path("/tmp")


def fault_scratch_path(name: str) -> str:
    """Path of one fault-injector backup file, private to the cluster this process drives.

    Concurrent experiments injecting the same fault on different clusters would
    otherwise read, overwrite, or apply each other's backups. Without a cluster
    name, the legacy ``/tmp/<name>`` path is used.
    """

    cluster = os.environ.get("SREGYM_KIND_CLUSTER_NAME", "").strip()
    directory = FAULT_SCRATCH_ROOT / f"sregym-{cluster}" if cluster else FAULT_SCRATCH_ROOT
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory / name)


def cluster_baseline_state_file() -> Path:
    """Baseline snapshot of the cluster this process drives.

    Keyed by ``SREGYM_KIND_CLUSTER_NAME`` so concurrent experiments on different
    clusters never load each other's baseline and reconcile away resources the
    other cluster needs. Without a cluster name, the legacy shared file is used.
    """

    cluster = os.environ.get("SREGYM_KIND_CLUSTER_NAME", "").strip()
    if not cluster:
        return CLUSTER_BASELINE_STATE_FILE
    return CACHE_DIR / f"cluster_baseline_state.{cluster}.json"

# Fault scripts
FAULT_SCRIPTS = BASE_DIR / "generators" / "fault" / "script"

# Metadata files
SOCIAL_NETWORK_METADATA = BASE_DIR / "service" / "metadata" / "social-network.json"
HOTEL_RES_METADATA = BASE_DIR / "service" / "metadata" / "hotel-reservation.json"
PROMETHEUS_METADATA = BASE_DIR / "service" / "metadata" / "prometheus.json"
LOKI_METADATA = BASE_DIR / "service" / "metadata" / "loki.json"
TRAIN_TICKET_METADATA = BASE_DIR / "service" / "metadata" / "train-ticket.json"
ASTRONOMY_SHOP_METADATA = BASE_DIR / "service" / "metadata" / "astronomy-shop.json"
TIDB_METADATA = BASE_DIR / "service" / "metadata" / "tidb-with-operator.json"
FLIGHT_TICKET_METADATA = BASE_DIR / "service" / "metadata" / "flight-ticket.json"
FLEET_CAST_METADATA = BASE_DIR / "service" / "metadata" / "fleet-cast.json"
BLUEPRINT_HOTEL_RES_METADATA = BASE_DIR / "service" / "metadata" / "blueprint-hotel-reservation.json"

# Khaos DaemonSet
KHAOS_DS = BASE_DIR / "service" / "khaos.yaml"

# MCP Server
MCP_SERVER_K8S = BASE_PARENT_DIR / "mcp_server" / "k8s"
