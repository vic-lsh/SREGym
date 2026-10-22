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
"""Fixtures shared by the conductor tests."""

import base64

import pytest
import yaml

_PEM = base64.b64encode(b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n").decode()


@pytest.fixture
def offline_kubeconfig(tmp_path, monkeypatch):
    """Conductor construction reads a kubeconfig but never contacts the cluster."""
    path = tmp_path / ".kube" / "config"
    path.parent.mkdir()
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "fake",
                "clusters": [
                    {"name": "fake", "cluster": {"server": "https://127.0.0.1:1", "certificate-authority-data": _PEM}}
                ],
                "users": [{"name": "fake", "user": {"client-certificate-data": _PEM, "client-key-data": _PEM}}],
                "contexts": [{"name": "fake", "context": {"cluster": "fake", "user": "fake"}}],
            }
        )
    )
    # Pointed at, not unset: a developer's KUBECONFIG must never be the one loaded.
    monkeypatch.setenv("KUBECONFIG", str(path))
    monkeypatch.setenv("HOME", str(tmp_path))
