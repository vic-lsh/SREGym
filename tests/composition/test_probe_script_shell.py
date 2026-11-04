"""The traffic probe script, run by a real POSIX sh against a stub ``wget`` on PATH.

The stub answers like busybox wget: ``-S`` prints the status line on stderr, a GET writes the body
to ``-O``. A POST's form is appended to a store unless DROP is set (the app answers 200 but loses
the write, like social-network's one-way compose); a GET of a timeline returns the store.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from sregym.conductor.problems.composition import grading

STUB = r"""#!/bin/sh
out=/dev/null; post=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -O) out="$2"; shift ;;
    --post-data) post="$2"; shift ;;
    -T) shift ;;
    -q|-S) ;;
    *) url="$1" ;;
  esac
  shift
done
if [ -n "$post" ]; then
  [ -z "$DROP" ] && echo "$post" >> "$STORE"
  echo "  HTTP/1.1 200 OK" >&2
  exit 0
fi
case "$url" in
  *timeline*) if [ "$out" = "-" ]; then cat "$STORE"; else cat "$STORE" > "$out"; fi ;;
  *) [ "$out" = "-" ] && echo "<html>" ;;
esac
exit 0
"""


def _run(tmp_path: Path, drop: bool) -> grading.ProbeResult:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    stub = bindir / "wget"
    stub.write_text(STUB)
    stub.chmod(0o755)
    (bindir / "sleep").symlink_to(shutil.which("true"))
    store = tmp_path / "store"
    store.write_text("")
    entry = grading.ENTRY_POINTS["Social Network"]
    script = grading.probe_script("social-network", entry, rounds=2)
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "STORE": str(store), **({"DROP": "1"} if drop else {})}
    log = subprocess.run(["sh", "-c", script], capture_output=True, text=True, env=env, timeout=60, check=True).stdout
    return grading.parse_probe_log(log)


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_a_landed_write_reads_back(tmp_path):
    result = _run(tmp_path, drop=False)
    assert result.ok and not result.failed_paths
    assert "pm" in (tmp_path / "store").read_text()  # the marker went into the posted text


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_a_write_answered_200_but_dropped_fails_the_probe(tmp_path):
    result = _run(tmp_path, drop=True)
    assert "POST /wrk2-api/post/compose" in result.failed_paths


def test_marker_field_and_readback_go_together():
    with pytest.raises(ValueError):
        grading.Request("/w", (("text", "x"),), marker_field="text")
    with pytest.raises(ValueError):
        grading.Request("/w", (("text", "x"),), marker_field="nope", readback="/r")
