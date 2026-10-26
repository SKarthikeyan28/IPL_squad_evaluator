from __future__ import annotations

import io
import zipfile
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from cricket_value.ingest.download import Source


def make_zip(members: dict[str, bytes]) -> bytes:
    """Build a zip in memory with fixed member timestamps, so equal inputs give equal bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0)), data)
    return buf.getvalue()


class FakeServer:
    """Serves one body per URL and records the requests it receives."""

    def __init__(self) -> None:
        self.bodies: dict[str, bytes] = {}
        self.last_modified: dict[str, str] = {}
        self.status_override: dict[str, int] = {}
        self.requests: list[httpx.Request] = []

    def set(self, url: str, body: bytes, last_modified: str | None = None) -> None:
        self.bodies[url] = body
        if last_modified is not None:
            self.last_modified[url] = last_modified

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        if url in self.status_override:
            return httpx.Response(self.status_override[url])
        headers = {}
        lm = self.last_modified.get(url)
        if lm is not None:
            headers["Last-Modified"] = lm
            if request.headers.get("If-Modified-Since") == lm:
                return httpx.Response(304, headers=headers)
        return httpx.Response(200, content=self.bodies[url], headers=headers)


@pytest.fixture
def server() -> FakeServer:
    return FakeServer()


@pytest.fixture
def client(server: FakeServer) -> Iterator[httpx.Client]:
    with httpx.Client(transport=httpx.MockTransport(server.handler)) as c:
        yield c


@pytest.fixture
def clock() -> Callable[[], datetime]:
    """A clock that advances one minute per call, so every fetch gets a distinct timestamp."""
    state = {"t": datetime(2026, 10, 26, 7, 0, 0, tzinfo=UTC)}

    def now() -> datetime:
        t = state["t"]
        state["t"] = t + timedelta(minutes=1)
        return t

    return now


IPL = Source(name="cricsheet_ipl", url="https://example.test/ipl_json.zip", kind="zip")
PEOPLE = Source(name="cricsheet_people", url="https://example.test/people.csv", kind="file")
