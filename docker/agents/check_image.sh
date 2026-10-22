#!/usr/bin/env bash
# Fail unless the agent image IMAGE holds no answer-key material and every
# containerized agent driver in agents.yaml imports cleanly inside it.
set -euo pipefail

IMAGE="${1:?usage: check_image.sh IMAGE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

read -r -d '' CHECK <<'EOF' || true
import importlib.util
import os
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

# Benchmark internals the agent must never read: fault definitions, graders,
# the conductor, fault generators, host-only agents (demo replays an answer).
FORBIDDEN_NAMES = {"sregym", "conductor", "problems", "oracles", "generators", "SREGym-applications"}
FORBIDDEN_PATHS = [Path("/opt/sregym/clients/demo"), Path("/opt/sregym/clients/tierzero"), Path("/root/.kube")]

errors = []
for root, dirs, files in os.walk("/opt/sregym"):
    for name in dirs + files:
        if Path(name).stem in FORBIDDEN_NAMES:
            errors.append(f"forbidden path in image: {Path(root) / name}")
errors += [f"forbidden path in image: {p}" for p in FORBIDDEN_PATHS if p.exists()]
errors += [f"fault backup in image: {p}" for p in Path("/tmp").glob("sregym*")]
if importlib.util.find_spec("sregym") is not None:
    errors.append("the sregym package is importable in the image")
errors += [f"image sets {k}" for k in os.environ if "KUBECONFIG" in k]

agents = yaml.safe_load(Path("/tmp/agents.yaml").read_text())["agents"]
for agent in agents:
    argv = shlex.split(agent.get("kickoff_command") or "")
    if not agent.get("container_isolation", True) or argv[:2] != ["python", "-m"]:
        continue
    module = argv[2]
    r = subprocess.run(
        [sys.executable, "-c", f"import {module}"], cwd="/opt/sregym", capture_output=True, text=True, timeout=120
    )
    if r.returncode:
        errors.append(f"{agent['name']}: import {module} failed:\n{r.stderr[-2000:]}")
    else:
        print(f"ok: {agent['name']} imports {module}")

for e in errors:
    print(f"FAIL: {e}", file=sys.stderr)
sys.exit(1 if errors else 0)
EOF

docker run --rm --network=none --entrypoint python3 \
    -v "$REPO_ROOT/agents.yaml:/tmp/agents.yaml:ro" \
    "$IMAGE" -c "$CHECK"
