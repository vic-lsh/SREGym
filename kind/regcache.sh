#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# regcache.sh: local pull-through image caches for kind clusters, so image
# pulls stop hitting Docker Hub rate limits and repeat pulls stay on the host.
#
# One CNCF distribution registry (registry:3, proxy mode) per upstream, on the
# kind docker network, no published ports. Nodes reach a cache by container
# name through containerd's /etc/containerd/certs.d/<registry>/hosts.toml.
#
# Usage:
#   regcache.sh up                 start or update the caches (idempotent)
#   regcache.sh creds [FILE]       load Docker Hub creds (line 1 user, line 2
#                                  token) into the docker.io cache
#   regcache.sh attach CLUSTER...  write hosts.toml on every node of the kind
#                                  clusters (no containerd restart)
#   regcache.sh status [CLUSTER...] caches, disk use, creds, attached clusters
#   regcache.sh warm [IMAGE...]    pull images (args, or one per line on stdin)
#                                  through the caches without storing them on
#                                  any node
#
# Environment:
#   REGCACHE_DIR      cache storage root (default ${XDG_CACHE_HOME:-~/.cache}/regcache);
#                     one subdirectory per upstream
#   REGCACHE_CREDS    Docker Hub creds file (default ~/.config/sregym/dockerhub)
#   REGCACHE_NETWORK  docker network the kind nodes are on (default kind)
#   REGCACHE_PREFIX   container name prefix (default rs-regcache)
#   REGCACHE_IMAGE    registry image (default registry:3)
#   REGCACHE_PLATFORM platform `warm` fetches (default linux/amd64)

set -euo pipefail

DIR="${REGCACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/regcache}"
CREDS="${REGCACHE_CREDS:-$HOME/.config/sregym/dockerhub}"
NETWORK="${REGCACHE_NETWORK:-kind}"
PREFIX="${REGCACHE_PREFIX:-rs-regcache}"
IMAGE="${REGCACHE_IMAGE:-registry:3}"
PLATFORM="${REGCACHE_PLATFORM:-linux/amd64}"

# name registry remote-url fallbacks (comma-separated, tried after the cache
# and before the registry itself)
UPSTREAMS=(
  "dockerhub docker.io https://registry-1.docker.io https://mirror.gcr.io"
  "ghcr ghcr.io https://ghcr.io -"
  "quay quay.io https://quay.io -"
  "k8s registry.k8s.io https://registry.k8s.io -"
)

die() { echo "regcache: $*" >&2; exit 1; }

# The UPSTREAMS entry whose name or registry is $1.
upstream() {
  local u f
  for u in "${UPSTREAMS[@]}"; do
    read -ra f <<<"$u"
    if [[ ${f[0]} == "$1" || ${f[1]} == "$1" ]]; then echo "$u"; return; fi
  done
  return 1
}

config_yml() { # remote-url
  cat <<EOF
version: 0.1
log:
  level: info
  accesslog:
    disabled: false
storage:
  filesystem:
    rootdirectory: /var/lib/registry
  delete:
    enabled: true
  cache:
    blobdescriptor: inmemory
http:
  addr: :5000
  debug:
    addr: :5001
    prometheus:
      enabled: true
      path: /metrics
health:
  storagedriver:
    enabled: true
    interval: 30s
    threshold: 3
proxy:
  remoteurl: $1
  # Keep cached content until removed by hand; the default expires it after 7 days.
  ttl: 0s
EOF
}

# Start (or recreate when its config, image or creds changed) one cache.
start_cache() { # name remote-url
  local name=$1 remote=$2 c="$PREFIX-$1" d="$DIR/$1"
  mkdir -p "$d/data"
  config_yml "$remote" >"$d/config.yml.new"
  mv "$d/config.yml.new" "$d/config.yml"
  local env=() envhash=none
  if [[ -s $d/proxy.env ]]; then
    env=(--env-file "$d/proxy.env")
    envhash=$(stat -c %Y "$d/proxy.env")
  fi
  local want
  want=$(cat "$d/config.yml" <(echo "$IMAGE $NETWORK $envhash") | sha256sum | cut -c1-16)
  local have
  have=$(docker inspect -f '{{index .Config.Labels "regcache.hash"}} {{.State.Running}}' "$c" 2>/dev/null || true)
  if [[ $have == "$want true" ]]; then
    echo "$c: up to date"
    return
  fi
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker run -d --name "$c" --restart unless-stopped --network "$NETWORK" \
    --log-opt max-size=20m --log-opt max-file=3 \
    --label "regcache.hash=$want" --label "regcache.remote=$remote" \
    -v "$d/config.yml:/etc/distribution/config.yml:ro" \
    -v "$d/data:/var/lib/registry" \
    "${env[@]}" "$IMAGE" >/dev/null
  echo "$c: started ($remote)"
}

cmd_up() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || die "docker network $NETWORK not found (create a kind cluster first)"
  mkdir -p "$DIR"
  local u name registry remote
  for u in "${UPSTREAMS[@]}"; do
    read -r name registry remote _ <<<"$u"
    start_cache "$name" "$remote"
  done
}

cmd_creds() {
  local file=${1:-$CREDS} d="$DIR/dockerhub"
  [[ -r $file ]] || die "no creds file at $file (line 1 user, line 2 token)"
  local user token
  { IFS= read -r user; IFS= read -r token; } <"$file" || true
  [[ -n $user && -n $token ]] || die "$file needs the user on line 1 and the token on line 2"
  mkdir -p "$d"
  (
    umask 077
    printf 'REGISTRY_PROXY_USERNAME=%s\nREGISTRY_PROXY_PASSWORD=%s\n' "$user" "$token" >"$d/proxy.env.new"
  )
  mv "$d/proxy.env.new" "$d/proxy.env"
  local f
  read -ra f <<<"$(upstream dockerhub)"
  start_cache dockerhub "${f[2]}"
  echo "docker.io cache now pulls as $user"
}

hosts_toml() { # registry name fallbacks
  local registry=$1 name=$2 fallbacks=$3 server f
  server="https://$registry"
  [[ $registry == docker.io ]] && server="https://registry-1.docker.io"
  echo "# Written by regcache.sh: local cache, then fallbacks, then the registry itself."
  echo "server = \"$server\""
  echo
  echo "[host.\"http://$PREFIX-$name:5000\"]"
  echo "  capabilities = [\"pull\", \"resolve\"]"
  [[ $fallbacks == - ]] && return
  for f in ${fallbacks//,/ }; do
    echo
    echo "[host.\"$f\"]"
    echo "  capabilities = [\"pull\", \"resolve\"]"
  done
}

cmd_attach() {
  [[ $# -gt 0 ]] || die "usage: regcache.sh attach CLUSTER..."
  local cluster node u name registry fallbacks
  for cluster in "$@"; do
    local nodes
    nodes=$(kind get nodes --name "$cluster")
    [[ -n $nodes ]] || die "kind cluster $cluster has no nodes"
    for node in $nodes; do
      docker inspect -f '{{json .NetworkSettings.Networks}}' "$node" | grep -q "\"$NETWORK\"" ||
        die "$node is not on docker network $NETWORK"
      for u in "${UPSTREAMS[@]}"; do
        read -r name registry _ fallbacks <<<"$u"
        # Write then rename, so containerd never reads a half-written file.
        hosts_toml "$registry" "$name" "$fallbacks" | docker exec -i "$node" sh -c \
          "d=/etc/containerd/certs.d/$registry && mkdir -p \$d && cat >\$d/.hosts.toml.new && mv \$d/.hosts.toml.new \$d/hosts.toml"
      done
      if docker exec "$node" grep -q 'config_path *= *"/etc/containerd/certs.d"' /etc/containerd/config.toml; then
        echo "$node: attached"
      else
        echo "$node: hosts.toml written, but containerd config_path is not /etc/containerd/certs.d; takes effect only after it is set and containerd restarts" >&2
      fi
    done
  done
}

cmd_status() {
  local u name remote
  row() { printf '%-22s %-8s %-7s %-6s %-9s %s\n' "$@"; }
  row CONTAINER STATE DISK CREDS HIT/MISS UPSTREAM
  for u in "${UPSTREAMS[@]}"; do
    read -r name _ remote _ <<<"$u"
    local c="$PREFIX-$name" state disk creds hits
    state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo absent)
    disk=$(du -sh "$DIR/$name/data" 2>/dev/null | cut -f1)
    creds=anon
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$c" 2>/dev/null |
      grep -q '^REGISTRY_PROXY_USERNAME=' && creds=auth
    # Blob hits/misses since the container started.
    hits=$(docker exec "$c" wget -qO- localhost:5001/metrics 2>/dev/null | awk '
      /^registry_proxy_hits_total\{type="blob"\}/ {h=$2}
      /^registry_proxy_misses_total\{type="blob"\}/ {m=$2}
      END {if (h != "") printf "%d/%d", h, m}' || true)
    row "$c" "$state" "${disk:--}" "$creds" "${hits:--}" "$remote"
  done
  if [[ -r $CREDS && ! -s $DIR/dockerhub/proxy.env ]]; then
    echo "note: $CREDS exists but is not applied; run: regcache.sh creds"
  fi
  local cluster node
  for cluster in "$@"; do
    for node in $(kind get nodes --name "$cluster"); do
      if docker exec "$node" grep -qs "$PREFIX-dockerhub" /etc/containerd/certs.d/docker.io/hosts.toml; then
        echo "$node: attached"
      else
        echo "$node: not attached"
      fi
    done
  done
}

# Fetch an image's manifest, config and layers through its cache, discarding
# the bytes; the cache keeps them.
warm_one() { # image
  local ref=$1 registry rest repo tag
  # Normalize to registry/repo:tag the way containerd does.
  if [[ $ref == */* && ${ref%%/*} == *[.:]* ]]; then
    registry=${ref%%/*}; rest=${ref#*/}
  else
    registry=docker.io; rest=$ref
  fi
  [[ $registry == docker.io && $rest != */* ]] && rest="library/$rest"
  repo=${rest%@*}; tag=latest
  if [[ ${repo##*/} == *:* ]]; then tag=${repo##*:}; repo=${repo%:*}; fi
  [[ $rest == *@* ]] && tag=${rest#*@}
  local f ip base
  read -ra f <<<"$(upstream "$registry")"
  [[ ${#f[@]} -gt 0 ]] || { echo "skip $ref: no cache for $registry"; return 0; }
  local name=${f[0]}
  ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$PREFIX-$name")
  base="http://$ip:5000/v2/$repo"
  local accept="application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"
  local m digest
  m=$(curl -fsS -H "Accept: $accept" "$base/manifests/$tag") || { echo "FAIL $ref: manifest"; return 1; }
  if jq -e '.manifests' <<<"$m" >/dev/null; then
    digest=$(jq -r --arg os "${PLATFORM%/*}" --arg arch "${PLATFORM#*/}" \
      '[.manifests[] | select(.platform.os == $os and .platform.architecture == $arch)][0].digest // empty' <<<"$m")
    [[ -n $digest ]] || { echo "FAIL $ref: no $PLATFORM manifest"; return 1; }
    m=$(curl -fsS -H "Accept: $accept" "$base/manifests/$digest") || { echo "FAIL $ref: platform manifest"; return 1; }
  fi
  local blob bytes total=0
  for blob in $(jq -r '.config.digest, .layers[].digest' <<<"$m"); do
    bytes=$(curl -fsSL -o /dev/null -w '%{size_download}' "$base/blobs/$blob") || { echo "FAIL $ref: blob $blob"; return 1; }
    total=$((total + ${bytes%.*}))
  done
  echo "ok $ref ($((total / 1048576)) MiB)"
}

cmd_warm() {
  command -v jq >/dev/null || die "warm needs jq"
  local images=("$@") failed=0 img
  if [[ ${#images[@]} -eq 0 ]]; then
    mapfile -t images < <(grep -v '^\s*\(#\|$\)')
  fi
  for img in "${images[@]}"; do
    warm_one "$img" || failed=$((failed + 1))
  done
  [[ $failed -eq 0 ]] || die "$failed image(s) failed; rerun to retry"
}

case "${1:-}" in
  up) shift; cmd_up "$@" ;;
  creds) shift; cmd_creds "$@" ;;
  attach) shift; cmd_attach "$@" ;;
  status) shift; cmd_status "$@" ;;
  warm) shift; cmd_warm "$@" ;;
  *) sed -n '/^# Usage:/,/^$/s/^# \{0,1\}//p' "$0"; exit 2 ;;
esac
