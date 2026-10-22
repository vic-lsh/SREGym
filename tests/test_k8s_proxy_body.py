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
"""The API proxy forwards request bodies whole, however the client framed them.

Regression: `kubectl replace --raw PATH -f FILE` sends its PUT body chunked, with no
Content-Length; the proxy read nothing and the request hung.
"""

import io
import random
from email.message import Message

import pytest

from sregym.service.k8s_proxy import read_request_body, upstream_request_headers


def _headers(**fields) -> Message:
    m = Message()
    for k, v in fields.items():
        m[k.replace("_", "-")] = v
    return m


def _chunked(body: bytes, sizes: list[int], extension: bool = False) -> bytes:
    out, at = b"", 0
    for n in sizes:
        piece = body[at : at + n]
        at += n
        if piece:
            out += f"{len(piece):x}{';ext=1' if extension else ''}\r\n".encode() + piece + b"\r\n"
    return out + b"0\r\n\r\n"


@pytest.mark.parametrize("seed", range(50))
def test_chunked_body_is_read_whole_for_any_chunking(seed):
    rng = random.Random(seed)
    body = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 5000)))
    sizes = [rng.randrange(1, 700) for _ in range(len(body) + 1)]
    wire = _chunked(body, sizes, extension=rng.random() < 0.3) + b"NEXT REQUEST"
    rfile = io.BytesIO(wire)
    assert read_request_body(_headers(Transfer_Encoding="chunked"), rfile) == body
    assert rfile.read() == b"NEXT REQUEST"  # exactly the body consumed


def test_content_length_body_and_no_body():
    assert read_request_body(_headers(Content_Length="5"), io.BytesIO(b"hello world")) == b"hello"
    assert read_request_body(_headers(), io.BytesIO(b"")) is None


def test_truncated_chunked_body_fails_loudly():
    with pytest.raises(ValueError):
        read_request_body(_headers(Transfer_Encoding="chunked"), io.BytesIO(b"a\r\nshort"))
    with pytest.raises(ValueError):
        read_request_body(_headers(Transfer_Encoding="chunked"), io.BytesIO(b"3\r\nabc\r\n"))


def test_framing_headers_are_not_forwarded():
    sent = _headers(Host="x", Transfer_Encoding="chunked", Content_Length="3", Accept="application/json")
    assert upstream_request_headers(sent) == {"Accept": "application/json"}
