"""Layered import: downloads, selection, atomic replacement, bounded work."""

import asyncio
import sqlite3
import subprocess
import sys
import time
import zipfile
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import override

import pytest
from aiohttp import ClientSession, ClientTimeout, web
from aiohttp.test_utils import TestServer

from conftest import FeedServer, Tables, base_tables, write_feed
from gtfs_hub import (
    GenericProvider,
    GtfsAuthError,
    GtfsConnectionError,
    GtfsFeedError,
    GtfsHub,
    GtfsRateLimitError,
    LineKey,
)
from gtfs_hub.download import async_download

LINE_11 = LineKey(3, "11")
LINE_12 = LineKey(3, "12")


def count(path: Path, sql: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute(sql).fetchone()[0])
    finally:
        conn.close()


def stop_times_of(path: Path, trip: str) -> int:
    return count(path, f"SELECT count(*) FROM stop_times WHERE trip_id = '{trip}'")


async def serve(handler: Callable[[web.Request], Awaitable[web.Response]]) -> TestServer:
    app = web.Application()
    app.router.add_get("/f", handler)
    server = TestServer(app)
    await server.start_server()
    return server


async def test_large_download_keeps_content(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    body = b"x" * (3 * 1024 * 1024 + 7)
    feed_server.files["big"] = body
    target = tmp_path / "big"
    await async_download(session, feed_server.url("big"), target)
    assert target.read_bytes() == body


async def test_download_timeout_is_a_connection_error(
    tmp_path: Path, session: ClientSession
) -> None:
    async def slow(request: web.Request) -> web.Response:
        await asyncio.sleep(5)
        return web.Response()

    server = await serve(slow)
    with pytest.raises(GtfsConnectionError):
        await async_download(
            session,
            str(server.make_url("/f")),
            tmp_path / "f",
            timeout=ClientTimeout(total=0.1),
        )
    await server.close()


@pytest.mark.parametrize(
    ("status", "headers", "error"),
    [
        (429, {"Retry-After": "30"}, GtfsRateLimitError),
        (401, {}, GtfsAuthError),
        (403, {}, GtfsAuthError),
        (500, {}, GtfsConnectionError),
        (404, {}, GtfsFeedError),
    ],
)
async def test_status_maps_to_typed_errors(
    tmp_path: Path,
    session: ClientSession,
    status: int,
    headers: dict[str, str],
    error: type[Exception],
) -> None:
    async def respond(request: web.Request) -> web.Response:
        return web.Response(status=status, headers=headers)

    server = await serve(respond)
    with pytest.raises(error) as caught:
        await async_download(session, str(server.make_url("/f")), tmp_path / "f")
    if isinstance(caught.value, GtfsRateLimitError):
        assert caught.value.retry_after == 30
    await server.close()


async def test_unreachable_server_is_a_connection_error(
    tmp_path: Path, session: ClientSession
) -> None:
    with pytest.raises(GtfsConnectionError):
        await async_download(session, "http://127.0.0.1:9/none", tmp_path / "f")


async def test_empty_selection_stores_catalogue_only(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([])
    assert await hub.async_search_lines("11")
    assert await hub.async_search_stops("Beta")
    assert count(path, "SELECT count(*) FROM agencies") == 1
    assert count(path, "SELECT count(*) FROM stop_times") == 0
    assert count(path, "SELECT count(*) FROM trips") == 0


async def test_only_selected_lines_are_stored(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    await GtfsHub(GenericProvider([url]), path, session, clock=now).async_sync([LINE_11])
    assert stop_times_of(path, "t1") == 2
    assert stop_times_of(path, "t2") == 0


async def test_all_route_ids_of_a_line_are_stored(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(
        routes=[("r11a", "a1", "11", "A", 3, ""), ("r11b", "a1", "11", "B", 3, "")],
        trips=[("r11a", "svc", "t1", 0), ("r11b", "svc", "t2", 1)],
    )
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    path = tmp_path / "db"
    await GtfsHub(GenericProvider([url]), path, session, clock=now).async_sync([LINE_11])
    assert count(path, "SELECT count(*) FROM trips") == 2


async def test_times_past_24_are_stored_as_seconds(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    await GtfsHub(GenericProvider([url]), path, session, clock=now).async_sync([LINE_12])
    assert count(path, "SELECT max(departure_seconds) FROM stop_times") == 25 * 3600 + 20 * 60


async def test_fares_and_unused_shapes_are_not_stored(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables()
    tables["shapes"] = ("shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence", [("x", 1, 2, 0)])
    tables["fare_attributes"] = ("fare_id,price", [("f", 1)])
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    path = tmp_path / "db"
    await GtfsHub(GenericProvider([url]), path, session, clock=now).async_sync([LINE_11])
    conn = sqlite3.connect(path)
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
    conn.close()
    assert "fare_attributes" not in names
    assert count(path, "SELECT count(*) FROM shape_points") == 0


@pytest.mark.parametrize(
    ("damage", "message"),
    [
        (lambda tables: tables.pop("stops"), "Missing stops"),
        (lambda tables: tables.update(routes=("route_id", [("r",)])), "route_type"),
    ],
)
async def test_bad_feed_is_a_feed_error(
    tmp_path: Path,
    feed_server: FeedServer,
    session: ClientSession,
    now: Callable[[], datetime],
    damage: Callable[[Tables], object],
    message: str,
) -> None:
    tables = base_tables()
    damage(tables)
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    with pytest.raises(GtfsFeedError, match=message):
        await hub.async_sync([])


async def test_not_a_zip_is_a_feed_error(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["a.zip"] = b"not a zip"
    hub = GtfsHub(GenericProvider([feed_server.url("a.zip")]), tmp_path / "db", session, clock=now)
    with pytest.raises(GtfsFeedError):
        await hub.async_sync([])


async def test_failed_import_leaves_data_and_no_temporary_files(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    good = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    store = tmp_path / "store"
    store.mkdir()
    path = store / "db"
    hub = GtfsHub(GenericProvider([good]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    before = path.read_bytes()
    tables = base_tables()
    tables["stop_times"] = (tables["stop_times"][0], [("t1", "bad", "bad", "s1", 0)])
    feed_server.put("b.zip", write_feed(tmp_path / "b.zip", tables))
    broken = GtfsHub(GenericProvider([good, feed_server.url("b.zip")]), path, session, clock=now)
    with pytest.raises(GtfsFeedError):
        await broken.async_sync([LINE_11])
    assert path.read_bytes() == before
    assert [item.name for item in store.iterdir()] == ["db"]
    assert await hub.async_stops_of_line(LINE_11)


async def test_adding_a_line_imports_only_that_line(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    conn = sqlite3.connect(path)
    original = conn.execute("SELECT rowid FROM stop_times WHERE trip_id = 't1'").fetchall()
    conn.close()
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    conn = sqlite3.connect(path)
    kept = conn.execute("SELECT rowid FROM stop_times WHERE trip_id = 't1'").fetchall()
    conn.close()
    assert kept == original
    assert stop_times_of(path, "t2") == 2
    assert count(path, "SELECT count(*) FROM snapshots") == 1


async def test_data_stays_readable_while_a_line_is_added(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    await hub.async_sync([LINE_11])
    task = asyncio.create_task(hub.async_sync([LINE_11, LINE_12]))
    while not task.done():
        assert await hub.async_stops_of_line(LINE_11)
    await task


async def test_removed_line_leaves_after_the_sync(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([LINE_11, LINE_12])
    await hub.async_sync([LINE_12])
    assert stop_times_of(path, "t1") == 0
    assert stop_times_of(path, "t2") == 2
    assert await hub.async_stops_of_line(LINE_11) == []


async def test_stop_of_weekday_snapshot_is_found_on_saturday(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    weekday = base_tables("20261102", "20261106")
    saturday = base_tables(
        "20261107",
        "20261107",
        stop_times=[("t1", "08:00:00", "08:00:00", "s1", 0)],
    )
    urls = [
        feed_server.put("w.zip", write_feed(tmp_path / "w.zip", weekday)),
        feed_server.put("s.zip", write_feed(tmp_path / "s.zip", saturday)),
    ]
    hub = GtfsHub(GenericProvider(urls), tmp_path / "db", session, clock=now)
    await hub.async_sync([LINE_11])
    assert {stop.name for stop in await hub.async_stops_of_line(LINE_11)} == {
        "Łódź Główna",
        "Stop Beta",
    }


async def test_slow_import_does_not_block_the_event_loop(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    class Slow(GenericProvider):
        @override
        def normalize_static(self, table: str, row: dict[str, str]) -> dict[str, str]:
            time.sleep(0.02)
            return row

    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    hub = GtfsHub(Slow([url]), tmp_path / "db", session, clock=now)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.005)
            ticks += 1

    task = asyncio.create_task(ticker())
    await hub.async_sync([LINE_11])
    task.cancel()
    assert ticks >= 10


_MEMORY_PROBE = """
import resource, sys
from datetime import date
from pathlib import Path
from gtfs_hub import GenericProvider, LineKey
from gtfs_hub.importer import apply_file
from gtfs_hub.models import StaticFeedRef
from gtfs_hub.schema import open_work

conn = open_work(Path(sys.argv[2]))
ref = StaticFeedRef("file", "v", date(2026, 11, 1), date(2026, 11, 7))
apply_file(conn, ref, Path(sys.argv[1]), [LineKey(3, "11")], GenericProvider(["x"]))
print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
"""


def _big_feed(path: Path, rows: int) -> Path:
    """Write a feed with half of its stop times on the selected line."""
    tables = base_tables()
    header = tables.pop("stop_times")[0]
    write_feed(path, tables)
    block = b"".join(
        f"{trip},08:00:00,08:00:00,s1,{n}\n".encode()
        for n in range(500)
        for trip in ("t1", "other")
    )
    with zipfile.ZipFile(path, "a", zipfile.ZIP_DEFLATED) as archive:  # noqa: SIM117
        with archive.open("stop_times.txt", "w", force_zip64=True) as member:
            member.write(f"{header}\n".encode())
            for _ in range(rows // 1000):
                member.write(block)
    return path


def _peak_memory(tmp_path: Path, rows: int) -> int:
    feed = _big_feed(tmp_path / f"{rows}.zip", rows)
    output = subprocess.run(
        [sys.executable, "-c", _MEMORY_PROBE, str(feed), str(tmp_path / f"{rows}.db")],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(output.stdout)


def test_peak_memory_does_not_follow_row_count(tmp_path: Path) -> None:
    small = _peak_memory(tmp_path, 1_000_000)
    large = _peak_memory(tmp_path, 2_000_000)
    print(f"peak memory: {small} and {large}")
    assert large < small * 1.2
