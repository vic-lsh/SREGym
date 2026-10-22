# Running SREGym with KIND

SREGym supports KIND on Linux, WSL2, and macOS. The provided setup creates one control-plane node, three worker nodes, and installs Calico for networking and NetworkPolicy support.

## Requirements

Install the dependencies listed in the [main README](../README.md): Python 3.12 or newer, Docker, KIND, kubectl, Helm 4.0 or newer, and uv.

## Linux and WSL2 host settings

KIND nodes share the host's inotify limits. On Linux and WSL2, low defaults can cause system pods to crash with `too many open files`.

Set the recommended values before creating the cluster:

```bash
sudo sysctl -w fs.inotify.max_user_instances=1024
sudo sysctl -w fs.inotify.max_user_watches=1048576
```

To keep the values after rebooting:

```bash
echo "fs.inotify.max_user_instances=1024" | sudo tee /etc/sysctl.d/99-sregym-kind.conf
echo "fs.inotify.max_user_watches=1048576" | sudo tee -a /etc/sysctl.d/99-sregym-kind.conf
sudo sysctl --system
```

These host commands are not needed on macOS.

## Create the cluster

From the repository root, run the command matching your machine:

```bash
# x86-64 Linux, WSL2, or Intel Mac
bash kind/setup_kind_cluster.sh x86

# ARM64 Linux or Apple silicon Mac
bash kind/setup_kind_cluster.sh arm
```

The script:

1. creates the four-node KIND cluster using the matching architecture image;
2. installs Calico;
3. waits for Calico and all nodes to become ready; and
4. clears the previous SREGym cluster-baseline cache.

Confirm that all four nodes are `Ready`:

```bash
kubectl get nodes
```

The cluster is now ready to run SREGym.

## (Optional) Local pull-through image cache

When several kind clusters share a host, `regcache.sh` runs one local
pull-through cache per registry (docker.io, ghcr.io, quay.io, registry.k8s.io)
so repeat pulls stay on the host and Docker Hub's per-IP rate limit (HTTP 429)
stops failing deploys. The caches are `registry:3` containers named
`rs-regcache-<upstream>` on the `kind` docker network, with no published ports.

```bash
export REGCACHE_DIR=/path/to/fast/disk/registry-cache  # default ~/.cache/regcache
bash kind/regcache.sh up                  # start the caches (idempotent)
bash kind/regcache.sh attach <cluster>    # write hosts.toml on every node
bash kind/regcache.sh status <cluster>    # disk use, hits/misses, attached nodes
```

`attach` only writes `/etc/containerd/certs.d/<registry>/hosts.toml`, which
containerd reads on the next pull, so it is safe on a cluster with running
workloads. Each node needs `config_path = "/etc/containerd/certs.d"` in its
containerd config (`attach` warns if it is missing). docker.io pulls try the
local cache, then `mirror.gcr.io`, then Docker Hub; the other registries try the
cache, then the registry itself, so a stopped cache only costs a retry. Run
`attach` after anything else that writes the docker.io hosts file.

To pull from Docker Hub as an account rather than anonymously, put the user on
line 1 and an access token on line 2 of `~/.config/sregym/dockerhub` (mode 600;
or pass another path), then run `bash kind/regcache.sh creds`. The token goes into
a mode-600 env file under `REGCACHE_DIR`, never on a command line.

`bash kind/regcache.sh warm < images.txt` fetches the listed images through the
caches without storing them on any node, so the next deploy pulls locally.

## Troubleshooting

### Docker issues

Ensure Docker is running and accessible to your user:

```bash
docker ps
```

### Cluster creation failures

Check that Docker is correctly installed and that your system has enough CPU and memory. Export the KIND logs for diagnostics:

```bash
kind export logs ./kind-logs --name kind
```

### Deployment problems

Inspect the pods and recent Kubernetes events, then use `kubectl logs <pod-name>` to view the logs for a failing pod:

```bash
kubectl get pods -A
kubectl get events -A --sort-by='.lastTimestamp'
```

### kube-proxy reports `CrashLoopBackOff` or `too many open files`

All KIND nodes share the host's `fs.inotify.max_user_instances` limit. When this limit is exhausted, new pods that need inotify instances, such as kube-proxy, crash immediately. Apply the Linux or WSL2 inotify settings above.

### Resource allocation

WSL2 may require additional resources. Adjust the WSL2 settings in your `.wslconfig` file on Windows if you encounter performance issues.

### Deployment timeout on a slow network

If you have a slow local network connection, first-time deployments may timeout while pulling container images. Increase the timeout in your `.env` file:

```bash
WAIT_FOR_POD_READY_TIMEOUT=1800  # 30 minutes (recommended for slow networks)
```

Subsequent deployments are faster since images are cached. Remote clusters typically don't need this adjustment.

### The cluster already exists

Delete the existing KIND cluster using the command below before recreating it.

## Delete the cluster

```bash
kind delete cluster --name kind
```

This removes the KIND node containers. Docker images remain cached.
