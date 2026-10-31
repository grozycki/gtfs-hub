"""Hand-made feeds and a local server, so no test touches the network."""

import zipfile
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

type Row = tuple[object, ...]
type Table = tuple[str, Sequence[Row]]
type Tables = dict[str, Table]


def write_feed(path: Path, tables: Mapping[str, Table]) -> Path:
    """Write a ZIP from tables given as a header line and rows."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, (header, rows) in tables.items():
            lines = [header, *(",".join(str(value) for value in row) for row in rows)]
            archive.writestr(f"{name}.txt", "\n".join(lines) + "\n")
    return path


def base_tables(
    start: str = "20261101",
    end: str = "20261107",
    routes: Sequence[Row] | None = None,
    stops: Sequence[Row] | None = None,
    trips: Sequence[Row] | None = None,
    stop_times: Sequence[Row] | None = None,
) -> Tables:
    """Two lines, three stops: line 11 serves A and B, line 12 serves B and C."""
    return {
        "agency": (
            "agency_id,agency_name,agency_url,agency_timezone",
            [("a1", "Agency One", "http://example.test", "Europe/Warsaw")],
        ),
        "feed_info": (
            "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date",
            [("Pub", "http://example.test", "pl", start, end)],
        ),
        "routes": (
            "route_id,agency_id,route_short_name,route_long_name,route_type,route_color",
            routes
            or [
                ("r11", "a1", "11", "Long Eleven", 3, "FF0000"),
                ("r12", "a1", "12", "Long Twelve", 3, ""),
            ],
        ),
        "stops": (
            "stop_id,stop_code,stop_name,stop_lat,stop_lon",
            stops
            or [
                ("s1", "C-A", "Łódź Główna", 50.1, 19.1),
                ("s2", "C-B", "Stop Beta", 50.2, 19.2),
                ("s3", "C-C", "Stop Gamma", 50.3, 19.3),
            ],
        ),
        "trips": (
            "route_id,service_id,trip_id,direction_id",
            trips or [("r11", "svc", "t1", 0), ("r12", "svc", "t2", 1)],
        ),
        "calendar": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date",
            [("svc", 1, 1, 1, 1, 1, 1, 1, start, end)],
        ),
        "stop_times": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence",
            stop_times
            or [
                ("t1", "08:00:00", "08:00:00", "s1", 0),
                ("t1", "08:05:00", "08:05:00", "s2", 1),
                ("t2", "25:10:00", "25:10:00", "s2", 0),
                ("t2", "25:20:00", "25:20:00", "s3", 1),
            ],
        ),
    }


@dataclass
class FeedServer:
    """Serves files by name and counts the requests for each."""

    base_url: str = ""
    files: dict[str, bytes] = field(default_factory=dict)
    hits: dict[str, int] = field(default_factory=dict)

    def url(self, name: str) -> str:
        return f"{self.base_url}/{name}"

    def put(self, name: str, source: Path) -> str:
        self.files[name] = source.read_bytes()
        return self.url(name)

    def total_hits(self) -> int:
        return sum(self.hits.values())


@pytest.fixture
async def feed_server() -> AsyncIterator[FeedServer]:
    state = FeedServer()

    async def handle(request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        state.hits[name] = state.hits.get(name, 0) + 1
        if name not in state.files:
            raise web.HTTPNotFound
        return web.Response(body=state.files[name])

    app = web.Application()
    app.router.add_get("/{name}", handle)
    server = TestServer(app)
    await server.start_server()
    state.base_url = str(server.make_url("")).rstrip("/")
    yield state
    await server.close()


@pytest.fixture
async def session() -> AsyncIterator[ClientSession]:
    async with ClientSession() as client:
        yield client


@pytest.fixture
def now() -> Callable[[], datetime]:
    """A fixed clock inside the default fixture window."""
    return lambda: datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
