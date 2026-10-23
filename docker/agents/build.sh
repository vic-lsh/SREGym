#!/usr/bin/env bash
# Build the agent image, or with --context DIR only assemble its build context.
#
# The image holds only what the containerized agent drivers run: an allowlist,
# never the sregym package (fault definitions, oracles, conductor). The agent
# must see the cluster only as an on-call engineer would, through the proxy.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# SREGym passes a tag naming the build context's hash, and the hash for the label
# it checks before use (sregym/service/container_runner.py).
IMAGE="${SREGYM_AGENT_IMAGE:-sregym-agent-slim:latest}"
CONTEXT_HASH="${SREGYM_AGENT_CONTEXT_HASH:-}"

# Agent packages for the containerized agents in agents.yaml. Host-only agents
# (demo, tierzero) stay out: demo ships a scripted answer.
CLIENT_PACKAGES=(harness autosubmit claudecode codex copilot geminicli opencode stratus)

assemble_context() {
    local ctx="$1"
    rm -rf "$ctx"
    mkdir -p "$ctx/clients"

    cp "$REPO_ROOT/clients/__init__.py" "$ctx/clients/__init__.py"
    for pkg in "${CLIENT_PACKAGES[@]}"; do
        cp -r "$REPO_ROOT/clients/$pkg" "$ctx/clients/$pkg"
    done
    cp -r "$REPO_ROOT/logger"      "$ctx/logger"
    cp -r "$REPO_ROOT/llm_backend" "$ctx/llm_backend"
    find "$ctx" -name __pycache__ -type d -prune -exec rm -rf {} +

    cp -r "$SCRIPT_DIR/install-scripts"         "$ctx/install-scripts"
    cp "$SCRIPT_DIR/requirements-container.txt" "$ctx/requirements-container.txt"
    cp "$SCRIPT_DIR/Dockerfile"                 "$ctx/Dockerfile"
}

if [ "${1:-}" = "--context" ]; then
    assemble_context "${2:?usage: build.sh --context DIR}"
    exit 0
fi

BUILD_CTX="$SCRIPT_DIR/build-context"
echo "==> Assembling build context..."
assemble_context "$BUILD_CTX"
trap 'rm -rf "$BUILD_CTX"' EXIT

# Capture the current image ID so we can remove it after the new build
OLD_IMAGE_ID="$(docker images -q "$IMAGE" 2>/dev/null || true)"

echo "==> Building Docker image..."
docker build --build-arg CACHE_BUST="$(date +%s)" --label "sregym.agent-context=$CONTEXT_HASH" \
    -t "$IMAGE" -f "$BUILD_CTX/Dockerfile" "$BUILD_CTX"

echo "==> Checking image contents..."
if ! "$SCRIPT_DIR/check_image.sh" "$IMAGE"; then
    echo "==> Image check failed; untagging $IMAGE" >&2
    docker rmi "$IMAGE" >/dev/null 2>&1 || true
    exit 1
fi

# Remove the previous image (now untagged) to avoid dangling buildup. Not when
# SREGym drives the build: another SREGym process may have checked that image
# and be about to run it by ID.
if [ -n "$OLD_IMAGE_ID" ] && [ -z "$CONTEXT_HASH" ]; then
    NEW_IMAGE_ID="$(docker images -q "$IMAGE")"
    if [ "$OLD_IMAGE_ID" != "$NEW_IMAGE_ID" ]; then
        echo "==> Removing previous image $OLD_IMAGE_ID..."
        docker rmi "$OLD_IMAGE_ID" 2>/dev/null || true
    fi
fi

echo "==> Done! Image: $IMAGE"
