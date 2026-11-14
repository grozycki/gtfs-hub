"""Searching and resolving stable keys across snapshots."""

import sqlite3
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from aiohttp import ClientSession

from conftest import FeedServer, Tables, base_tables, write_feed
from gtfs_hub import GenericProvider, GtfsHub, LineKey, ResolvedId, StopKey, StopKeyField
from gtfs_hub.catalogue import STOP_CHOICES_SQL


async def make_hub(
    tmp_path: Path,
    feed_server: FeedServer,
    session: ClientSession,
    now: Callable[[], datetime],
    *feeds: Tables,
    stop_key: StopKeyField = "stop_id",
) -> GtfsHub:
    urls = [
        feed_server.put(f"{n}.zip", write_feed(tmp_path / f"{n}.zip", tables))
        for n, tables in enumerate(feeds)
    ]
    provider = GenericProvider(urls, stop_key=stop_key)
    hub = GtfsHub(provider, tmp_path / "db", session, clock=now)
    await hub.async_sync([])
    return hub


async def test_bus_and_tram_with_one_number_are_two_lines(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(
        routes=[("bus", "a1", "11", "Bus", 3, ""), ("tram", "a1", "11", "Tram", 0, "")]
    )
    hub = await make_hub(tmp_path, feed_server, session, now, tables)
    found = await hub.async_search_lines("11")
    assert {line.key for line in found} == {LineKey(3, "11"), LineKey(0, "11")}
    assert await hub.async_resolve_line(LineKey(0, "11")) == [ResolvedId(1, "tram")]


async def test_changed_route_id_resolves_under_one_key(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    first = base_tables("20261101", "20261103", routes=[("old", "a1", "11", "L", 3, "")])
    second = base_tables("20261104", "20261107", routes=[("new", "a1", "11", "L", 3, "")])
    hub = await make_hub(tmp_path, feed_server, session, now, first, second)
    resolved = await hub.async_resolve_line(LineKey(3, "11"))
    assert [item.id for item in resolved] == ["old", "new"]
    assert len(await hub.async_search_lines("11")) == 1


async def test_stop_key_with_two_ids(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    first = base_tables("20261101", "20261103", stops=[("621", "C-X", "Stop", 50.0, 19.0)])
    second = base_tables("20261104", "20261107", stops=[("12957", "C-X", "Stop", 50.0, 19.0)])
    hub = await make_hub(tmp_path, feed_server, session, now, first, second, stop_key="stop_code")
    assert [item.id for item in await hub.async_resolve_stop(StopKey("C-X"))] == ["621", "12957"]
    [stop] = await hub.async_search_stops("stop")
    assert stop.key == "C-X"


async def test_absent_keys_resolve_to_nothing(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, base_tables())
    assert await hub.async_resolve_stop(StopKey("gone")) == []
    assert await hub.async_resolve_line(LineKey(3, "99")) == []


async def test_stop_missing_from_one_snapshot_stays(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    weekday = base_tables("20261102", "20261106")
    weekend = base_tables("20261107", "20261108", stops=[("s1", "C-A", "Other", 50.1, 19.1)])
    hub = await make_hub(tmp_path, feed_server, session, now, weekday, weekend)
    assert await hub.async_search_stops("Gamma")
    assert await hub.async_resolve_stop(StopKey("s3"))


async def test_line_search_by_number_and_long_name(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(
        routes=[
            ("r1", "a1", "111", "Other", 3, ""),
            ("r2", "a1", "11", "Long Eleven", 3, "FF0000"),
        ]
    )
    hub = await make_hub(tmp_path, feed_server, session, now, tables)
    found = await hub.async_search_lines("11")
    assert [line.key.short_name for line in found] == ["11", "111"]
    assert found[0].agency == "Agency One"
    assert found[0].long_name == "Long Eleven"
    assert [line.key.short_name for line in await hub.async_search_lines("eleven")] == ["11"]


async def test_stop_search_ignores_case_and_diacritics(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, base_tables())
    for query in ("łódź", "ŁÓDŹ", "lodz", "główna"):
        [stop] = await hub.async_search_stops(query)
        assert stop.name == "Łódź Główna"
    [stop] = await hub.async_search_stops("c-b")
    assert (stop.key, stop.code, stop.latitude) == ("s2", "C-B", 50.2)


async def test_search_treats_wildcards_as_text(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, base_tables())
    assert await hub.async_search_stops("%") == []
    assert await hub.async_search_lines("_") == []


async def test_search_gives_one_result_per_key(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    first = base_tables("20261101", "20261103")
    second = base_tables("20261104", "20261107")
    hub = await make_hub(tmp_path, feed_server, session, now, first, second)
    assert len(await hub.async_search_stops("beta")) == 1
    assert len(await hub.async_search_lines("11")) == 1


async def test_empty_colour_stays_absent(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, base_tables())
    assert (await hub.async_search_lines("12"))[0].color is None
    assert (await hub.async_search_lines("11"))[0].color == "FF0000"


async def test_stop_search_filters_before_the_limit(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, base_tables())
    found = await hub.async_search_stops("stop", limit=1, only={StopKey("s3")})
    assert [stop.key for stop in found] == ["s3"]


async def test_lines_in_natural_order(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(
        routes=[
            ("a", "a1", "10", "", 3, ""),
            ("b", "a1", "2", "", 3, ""),
            ("c", "a1", "A", "", 3, ""),
            ("d", "a1", "T6", "", 0, ""),
        ]
    )
    hub = await make_hub(tmp_path, feed_server, session, now, tables)
    assert [line.key.short_name for line in await hub.async_list_lines()] == [
        "2",
        "10",
        "A",
        "T6",
    ]


async def test_stop_choices_tell_two_posts_apart(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(
        stops=[
            ("s1", "C-1", "Same", 50.0, 19.0),
            ("s2", "C-2", "Same", 50.0, 19.0),
            ("s3", "C-3", "End", 50.0, 19.0),
            ("s4", "C-4", "Technical", 50.0, 19.0),
        ],
        trips=[("r11", "svc", "t1", 0), ("r11", "svc", "t2", 1)],
    )
    tables["trips"] = (
        "route_id,service_id,trip_id,direction_id,trip_headsign",
        [("r11", "svc", "t1", 0, "North"), ("r11", "svc", "t2", 1, "South")],
    )
    tables["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type",
        [
            ("t1", "08:00:00", "08:00:00", "s1", 0, 0, 0),
            ("t1", "08:05:00", "08:05:00", "s4", 1, 1, 1),
            ("t1", "08:10:00", "08:10:00", "s3", 2, 0, 0),
            ("t2", "09:00:00", "09:00:00", "s2", 0, 0, 0),
            ("t2", "09:10:00", "09:10:00", "s3", 1, 0, 0),
        ],
    )
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    line = LineKey(3, "11")
    await hub.async_sync([line])
    choices = {choice.stop.key: choice for choice in await hub.async_stop_choices([line])}
    assert set(choices) == {"s1", "s2", "s3"}
    assert choices[StopKey("s1")].headsigns == ("North",)
    assert choices[StopKey("s2")].headsigns == ("South",)
    assert choices[StopKey("s1")].lines == ("11",)


async def test_stop_choice_has_no_direction_where_trips_end(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = base_tables(trips=[("r11", "svc", "t1", 0)])
    tables["trips"] = (
        "route_id,service_id,trip_id,direction_id,trip_headsign",
        [("r11", "svc", "t1", 0, "End")],
    )
    tables["stop_times"] = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence",
        [("t1", "08:00:00", "08:00:00", "s1", 0), ("t1", "08:10:00", "08:10:00", "s2", 1)],
    )
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=now)
    line = LineKey(3, "11")
    await hub.async_sync([line])
    choices = {choice.stop.key: choice for choice in await hub.async_stop_choices([line])}
    assert choices[StopKey("s1")].headsigns == ("End",)
    assert choices[StopKey("s2")].headsigns == ()


async def test_stop_choices_cover_every_snapshot(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    later = base_tables(
        start="20261101",
        end="20261107",
        stop_times=[
            ("t1", "08:00:00", "08:00:00", "s3", 0),
            ("t1", "08:05:00", "08:05:00", "s2", 1),
            ("t2", "25:10:00", "25:10:00", "s2", 0),
            ("t2", "25:20:00", "25:20:00", "s3", 1),
        ],
    )
    urls = [
        feed_server.put(f"{n}.zip", write_feed(tmp_path / f"{n}.zip", tables))
        for n, tables in enumerate((base_tables(), later))
    ]
    hub = GtfsHub(GenericProvider(urls), tmp_path / "db", session, clock=now)
    line = LineKey(3, "11")
    await hub.async_sync([line])
    keys = {choice.stop.key for choice in await hub.async_stop_choices([line])}
    assert keys == {"s1", "s2", "s3"}


async def test_stop_choices_read_only_the_lines_trips(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    await GtfsHub(GenericProvider([url]), path, session, clock=now).async_sync(
        [LineKey(3, "11"), LineKey(3, "12")]
    )
    # With statistics of four rows a scan is right, so the plan is pinned for a planner that
    # has none, where the old query walked every stop time of the snapshot.
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE sqlite_stat1")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(path)
    plan = " ".join(
        row[3] for row in conn.execute(f"EXPLAIN QUERY PLAN {STOP_CHOICES_SQL}", (3, "11"))
    )
    conn.close()
    assert "stop_times_trip" in plan
    assert "stop_times_stop" not in plan
