"""Departures from the schedule, with hand-made feeds."""

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from aiohttp import ClientSession

from conftest import FeedServer, Tables, base_tables, write_feed
from gtfs_hub import DepartureStatus, GenericProvider, GtfsHub, GtfsNoDataError, LineKey, StopKey
from gtfs_hub.departures import service_instant

LINE_11 = LineKey(3, "11")
LINE_12 = LineKey(3, "12")
WARSAW = ZoneInfo("Europe/Warsaw")


def at(year: int, month: int, day: int, hour: int, minute: int) -> Callable[[], datetime]:
    moment = datetime(year, month, day, hour, minute, tzinfo=UTC)
    return lambda: moment


async def make_hub(
    tmp_path: Path,
    server: FeedServer,
    session: ClientSession,
    clock: Callable[[], datetime],
    *feeds: Tables,
    lines: tuple[LineKey, ...] = (LINE_11, LINE_12),
) -> GtfsHub:
    urls = [
        server.put(f"{n}.zip", write_feed(tmp_path / f"{n}.zip", tables))
        for n, tables in enumerate(feeds)
    ]
    hub = GtfsHub(GenericProvider(urls), tmp_path / "db", session, clock=clock)
    await hub.async_sync(list(lines))
    return hub


def test_time_past_midnight() -> None:
    moment = service_instant(date(2026, 11, 2), 25 * 3600 + 600, WARSAW)
    assert moment == datetime(2026, 11, 3, 0, 10, tzinfo=UTC)


def test_dst_night_counts_from_noon() -> None:
    assert service_instant(date(2026, 10, 25), 8 * 3600, WARSAW) == datetime(
        2026, 10, 25, 7, 0, tzinfo=UTC
    )
    assert service_instant(date(2026, 10, 25), 3600, WARSAW) == datetime(
        2026, 10, 25, 0, 0, tzinfo=UTC
    )


async def test_ordered_with_all_fields(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables(
        stop_times=[
            ("t1", "09:00:00", "09:00:00", "s2", 0),
            ("t1", "09:10:00", "09:10:00", "s3", 1),
            ("t2", "08:00:00", "08:00:00", "s2", 0),
            ("t2", "08:10:00", "08:10:00", "s3", 1),
        ]
    )
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 6, 0), tables)
    first, second = await hub.async_departures(StopKey("s2"))
    assert first.line == LINE_12
    assert second.line == LINE_11
    assert first.scheduled == datetime(2026, 11, 2, 7, 0, tzinfo=UTC)
    assert first.predicted is None
    assert first.delay is None
    assert first.status is DepartureStatus.SCHEDULED
    assert first.service_date == date(2026, 11, 2)
    assert len(await hub.async_departures(StopKey("s2"), limit=1)) == 1


async def test_past_departure_is_left_out(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 12, 0), base_tables())
    assert all(
        d.line != LINE_11
        for d in await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=1))
    )


async def test_previous_service_day_reaches_past_midnight(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 3, 0, 0), base_tables())
    departures = await hub.async_departures(StopKey("s2"))
    night = [d for d in departures if d.trip_id == "t2"]
    assert night[0].service_date == date(2026, 11, 2)
    assert night[0].scheduled == datetime(2026, 11, 3, 0, 10, tzinfo=UTC)


async def test_trip_in_two_snapshots_gives_one_departure(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    first = base_tables("20261101", "20261103")
    second = base_tables("20261102", "20261107")
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), first, second)
    departures = await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6))
    assert [(d.trip_id, d.service_date) for d in departures] == [("t1", date(2026, 11, 2))]


async def test_service_not_running_is_left_out(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    tables["calendar_dates"] = ("service_id,date,exception_type", [("svc", "20261102", 2)])
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6)) == []


async def test_weekday_flag(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    header = tables["calendar"][0]
    tables["calendar"] = (header, [("svc", 0, 1, 1, 1, 1, 1, 1, "20261101", "20261107")])
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6)) == []


async def test_feed_with_exceptions_only(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    del tables["calendar"]
    tables["calendar_dates"] = ("service_id,date,exception_type", [("svc", "20261102", 1)])
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert len(await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6))) == 1


async def test_no_boarding_and_last_stop_give_no_departure(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    tables["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type",
        [
            ("t1", "08:00:00", "08:00:00", "s1", 0, 1, 1),
            ("t1", "08:05:00", "08:05:00", "s2", 1, 0, 0),
            ("t1", "08:10:00", "08:10:00", "s3", 2, "", ""),
        ],
    )
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    horizon = timedelta(hours=6)
    assert await hub.async_departures(StopKey("s1"), horizon=horizon) == []
    assert len(await hub.async_departures(StopKey("s2"), horizon=horizon)) == 1
    assert await hub.async_departures(StopKey("s3"), horizon=horizon) == []
    stops = await hub.async_stops_of_line(LINE_11)
    assert {stop.key for stop in stops} == {"s2", "s3"}


async def test_expired_schedule_is_an_error(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    now = [datetime(2026, 11, 2, 5, 0, tzinfo=UTC)]
    hub = await make_hub(tmp_path, feed_server, session, lambda: now[0], base_tables())
    now[0] = datetime(2026, 12, 1, 5, 0, tzinfo=UTC)
    with pytest.raises(GtfsNoDataError):
        await hub.async_departures(StopKey("s1"))


async def test_headway_trips_are_left_out_and_logged(
    tmp_path: Path,
    feed_server: FeedServer,
    session: ClientSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    tables = base_tables()
    tables["frequencies"] = (
        "trip_id,start_time,end_time,headway_secs",
        [("t1", "06:00:00", "10:00:00", 600)],
    )
    with caplog.at_level(logging.WARNING):
        hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert "headway" in caplog.text
    assert await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6)) == []


async def test_source_links(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    tables["routes"] = (
        "route_id,agency_id,route_short_name,route_long_name,route_type,route_url",
        [("r11", "a1", "11", "L", 3, "http://example.test/11"), ("r12", "a1", "12", "L", 3, "")],
    )
    tables["stops"] = (
        "stop_id,stop_code,stop_name,stop_lat,stop_lon,stop_url",
        [("s1", "A", "One", 1, 2, "http://example.test/s1"), ("s2", "B", "Two", 1, 2, "")],
    )
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert (await hub.async_search_lines("11"))[0].url == "http://example.test/11"
    assert (await hub.async_search_lines("12"))[0].url is None
    assert (await hub.async_search_stops("One"))[0].url == "http://example.test/s1"
    assert (await hub.async_search_stops("Two"))[0].url is None


async def test_arrival_only_stop_time_gives_a_departure(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables(
        stop_times=[("t1", "08:00:00", "", "s1", 0), ("t1", "08:05:00", "08:05:00", "s2", 1)]
    )
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert len(await hub.async_departures(StopKey("s1"), horizon=timedelta(hours=6))) == 1


async def test_routes_without_agency_id_use_the_only_agency(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    tables = base_tables()
    tables["routes"] = (
        "route_id,route_short_name,route_long_name,route_type",
        [("r11", "11", "L", 3), ("r12", "12", "L", 3)],
    )
    hub = await make_hub(tmp_path, feed_server, session, at(2026, 11, 2, 5, 0), tables)
    assert (await hub.async_search_lines("11"))[0].agency == "Agency One"
