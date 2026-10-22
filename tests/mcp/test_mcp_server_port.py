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
"""MCPServer's stale-forward cleanup only ever targets listeners on its own configured port."""

import subprocess

from sregym.service.mcp_server import MCPServer


class FakeHost:
    """In-memory host: which pids listen on which TCP port, and which pids were killed."""

    def __init__(self, listeners: dict[int, list[int]]):
        self.listeners = listeners
        self.killed: list[int] = []
        self.commands: list[list[str]] = []

    def run(self, argv, **kwargs):
        assert isinstance(argv, list), "commands must be argv lists, not shell strings"
        self.commands.append(argv)
        if argv[0] == "lsof":
            assert "-t" in argv and "-sTCP:LISTEN" in argv, argv
            (spec,) = [a for a in argv if a.startswith("-iTCP:")]
            port = int(spec.removeprefix("-iTCP:"))
            pids = self.listeners.get(port, [])
            return subprocess.CompletedProcess(argv, 0 if pids else 1, "".join(f"{p}\n" for p in pids), "")
        if argv[0] == "kill":
            (pid,) = argv[1:]
            self.killed.append(int(pid))
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(f"unexpected command {argv}")


class FakeKubectl:
    def exec_command(self, command):
        raise AssertionError("cleanup must not touch the cluster")


def test_default_port_is_unchanged():
    assert MCPServer(kubectl=FakeKubectl()).port == 9954


def test_kills_only_listeners_on_its_own_port():
    host = FakeHost({9954: [111], 9955: [222, 223], 9956: [333]})
    server = MCPServer(port=9955, kubectl=FakeKubectl(), run=host.run)

    server._kill_stale_port_forward()  # the cleanup step start_port_forward runs; no public entry without a cluster

    assert host.killed == [222, 223]


def test_no_listener_kills_nothing():
    host = FakeHost({9954: [111]})
    server = MCPServer(port=9957, kubectl=FakeKubectl(), run=host.run)

    server._kill_stale_port_forward()

    assert host.killed == []
