"""Snapshot rules, and the schema behaviour they rest on."""

import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from aiohttp import ClientSession

from conftest import FeedServer, base_tables, write_feed
from gtfs_hub import (
    BaseProvider,
    GenericProvider,
    GtfsConnectionError,
    GtfsFeedError,
    GtfsHub,
    GtfsNoDataError,
    LineKey,
    Snapshot,
    StaticFeedRef,
)
from gtfs_hub.schema import SCHEMA_VERSION
from gtfs_hub.snapshots import is_expired, missing_dates, select_snapshot

LINE_11 = LineKey(3, "11")


def snap(id_: int, start: date, end: date) -> Snapshot:
    return Snapshot(id_, None, "x", start, end)


class Listed(BaseProvider):
    """A provider whose listing tests set directly."""

    key = "listed"
    name = "Listed"

    def __init__(self, refs: list[StaticFeedRef]) -> None:
        self.refs = refs
        self.failure: Exception | None = None

    async def async_list_static_feeds(self, session: ClientSession) -> list[StaticFeedRef]:
        if self.failure:
            raise self.failure
        return self.refs


def test_one_snapshot_covers_the_date() -> None:
    snapshots = [snap(1, date(2026, 11, 1), date(2026, 11, 3))]
    assert select_snapshot(snapshots, date(2026, 11, 2)) == snapshots[0]


def test_newest_wins_on_overlap() -> None:
    older = snap(1, date(2026, 11, 1), date(2026, 11, 5))
    newer = snap(2, date(2026, 11, 2), date(2026, 11, 4))
    assert select_snapshot([newer, older], date(2026, 11, 3)) == newer


def test_gap_is_reported() -> None:
    snapshots = [
        snap(1, date(2026, 11, 1), date(2026, 11, 2)),
        snap(2, date(2026, 11, 4), date(2026, 11, 5)),
    ]
    assert select_snapshot(snapshots, date(2026, 11, 3)) is None
    assert missing_dates(snapshots, date(2026, 11, 1), date(2026, 11, 5)) == [date(2026, 11, 3)]


def test_night_service_keeps_a_finished_window() -> None:
    end = date(2026, 11, 1)
    assert not is_expired(end, 27 * 3600, datetime(2026, 11, 2, 2, 0))
    assert is_expired(end, 27 * 3600, datetime(2026, 11, 2, 3, 0))


def test_window_ended_before_today_expires() -> None:
    assert is_expired(date(2026, 11, 1), 24 * 3600, datetime(2026, 11, 3, 0, 0))


def test_unknown_service_length_is_kept_longer() -> None:
    end = date(2026, 11, 1)
    assert not is_expired(end, None, datetime(2026, 11, 2, 5, 0))
    assert is_expired(end, None, datetime(2026, 11, 2, 6, 0))


async def test_window_from_feed_info(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    [snapshot] = await hub.async_snapshots()
    assert (snapshot.valid_from, snapshot.valid_to) == (date(2026, 11, 1), date(2026, 11, 7))


async def test_window_from_calendar(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables()
    del tables["feed_info"]
    tables["calendar"] = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date",
        [("svc", 1, 1, 1, 1, 1, 1, 1, "20261102", "20261108")],
    )
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    [snapshot] = await hub.async_snapshots()
    assert (snapshot.valid_from, snapshot.valid_to) == (date(2026, 11, 2), date(2026, 11, 8))


async def test_file_without_a_window_is_refused(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables()
    del tables["feed_info"]
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    with pytest.raises(GtfsFeedError, match="window"):
        await hub.async_sync([])


async def test_same_trip_id_stays_separate_per_snapshot(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    one = write_feed(tmp_path / "1.zip", base_tables("20261101", "20261103"))
    two = write_feed(tmp_path / "2.zip", base_tables("20261104", "20261107"))
    urls = [feed_server.put("1.zip", one), feed_server.put("2.zip", two)]
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider(urls), path, session, clock=now)
    await hub.async_sync([LINE_11])
    conn = sqlite3.connect(path)
    rows = conn.execute("SELECT snapshot, trip_id FROM trips ORDER BY snapshot").fetchall()
    conn.close()
    assert [trip for _, trip in rows] == ["t1", "t1"]
    assert len({snapshot for snapshot, _ in rows}) == 2


async def test_covered_date_without_trips_is_not_a_gap(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    assert await hub.async_missing_dates(date(2026, 11, 2), date(2026, 11, 8)) == [
        date(2026, 11, 8)
    ]
    selected = await hub.async_snapshot_for(date(2026, 11, 3))
    assert selected is not None


async def test_matching_version_is_not_downloaded(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, tmp_path / "db", session, clock=now)
    assert (await hub.async_sync([LINE_11])).imported == 1
    result = await hub.async_sync([LINE_11])
    assert (result.imported, result.skipped) == (0, 1)
    assert feed_server.total_hits() == 1


async def test_matching_hash_under_a_new_version_is_not_imported(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed = write_feed(tmp_path / "a.zip", base_tables())
    url = feed_server.put("a.zip", feed)
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, tmp_path / "db", session, clock=now)
    await hub.async_sync([LINE_11])
    provider.refs = [StaticFeedRef(feed_server.put("b.zip", feed), version="v2")]
    result = await hub.async_sync([LINE_11])
    assert (result.imported, result.relabelled) == (0, 1)
    [snapshot] = await hub.async_snapshots()
    assert snapshot.version == "v2"


async def test_expired_snapshot_is_removed(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    old = write_feed(tmp_path / "old.zip", base_tables("20261025", "20261030"))
    new = write_feed(tmp_path / "new.zip", base_tables("20261031", "20261107"))
    urls = [feed_server.put("old.zip", old), feed_server.put("new.zip", new)]
    hub = GtfsHub(
        GenericProvider(urls),
        tmp_path / "db",
        session,
        clock=lambda: datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
    )
    result = await hub.async_sync([])
    assert result.removed == 1
    [snapshot] = await hub.async_snapshots()
    assert snapshot.valid_from == date(2026, 10, 31)


async def test_previous_day_stays_during_night_service(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    """Trip t2 ends at 25:20, so its day lasts until 01:20 local time, 00:20 UTC in November."""
    old = write_feed(tmp_path / "a.zip", base_tables("20261025", "20261101"))
    url = feed_server.put("a.zip", old)
    line = LineKey(3, "12")
    clock_time = datetime(2026, 11, 2, 0, 10, tzinfo=UTC)
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=lambda: clock_time)
    assert (await hub.async_sync([line])).removed == 0
    assert len(await hub.async_snapshots()) == 1
    clock_time = datetime(2026, 11, 2, 0, 30, tzinfo=UTC)
    assert (await hub.async_sync([line])).removed == 1


async def test_file_gone_from_listing_goes_once_covered(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    first = write_feed(tmp_path / "1.zip", base_tables("20261101", "20261107"))
    url_one = feed_server.put("1.zip", first)
    provider = Listed([StaticFeedRef(url_one, version="v1")])
    hub = GtfsHub(provider, tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    partial = write_feed(tmp_path / "2.zip", base_tables("20261102", "20261103"))
    provider.refs = [StaticFeedRef(feed_server.put("2.zip", partial), version="v2")]
    assert (await hub.async_sync([])).removed == 0
    full = write_feed(tmp_path / "3.zip", base_tables("20261102", "20261110"))
    provider.refs.append(StaticFeedRef(feed_server.put("3.zip", full), version="v3"))
    assert (await hub.async_sync([])).removed == 1


async def test_failed_listing_keeps_data(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    provider.failure = GtfsConnectionError("down")
    with pytest.raises(GtfsConnectionError):
        await hub.async_sync([])
    provider.failure = None
    provider.refs = []
    with pytest.raises(GtfsFeedError):
        await hub.async_sync([])
    assert len(await hub.async_snapshots()) == 1


async def test_rebuild_on_schema_mismatch(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([])
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    conn.close()
    with pytest.raises(GtfsNoDataError):
        await hub.async_snapshots()
    assert (await hub.async_sync([])).imported == 1
    assert len(await hub.async_snapshots()) == 1


async def test_rebuild_on_missing_file(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([])
    path.unlink()
    assert (await hub.async_sync([])).imported == 1
    assert path.exists()
