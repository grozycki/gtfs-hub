"""Shapes: storage that follows the selection, queries and location on a path."""

import sqlite3
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from aiohttp import ClientSession

from conftest import FeedServer, Row, Tables, base_tables, write_feed
from gtfs_hub import GenericProvider, GtfsHub, LineKey, Shape, ShapeId, locate
from test_vehicles import Identified, positions

LINE_11 = LineKey(3, "11")
LINE_12 = LineKey(3, "12")
SHAPE_HEADER = "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence"
# About 70 m between points, heading east.
EAST = [(50.0, 19.0 + step * 0.001) for step in range(6)]


def shaped(
    shapes: dict[str, list[tuple[float, float]]],
    trips: list[Row] | None = None,
    start: str = "20261101",
) -> Tables:
    tables = base_tables(
        start=start,
        trips=trips
        or [
            ("r11", "svc", "t1", 0, "east", "Beta"),
            ("r11", "svc", "t3", 1, "west", "Alpha"),
            ("r12", "svc", "t2", 1, "other", "Gamma"),
        ],
    )
    tables["trips"] = (
        "route_id,service_id,trip_id,direction_id,shape_id,trip_headsign",
        tables["trips"][1],
    )
    tables["shapes"] = (
        SHAPE_HEADER,
        [
            (shape_id, lat, lon, sequence)
            for shape_id, points in shapes.items()
            for sequence, (lat, lon) in enumerate(points)
        ],
    )
    return tables


PATHS = {"east": EAST, "west": EAST[::-1], "other": [(51.0, 20.0), (51.0, 20.01)]}


def count(path: Path, sql: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute(sql).fetchone()[0])
    finally:
        conn.close()


async def synced(
    tmp_path: Path,
    server: FeedServer,
    session: ClientSession,
    now: Callable[[], datetime],
    tables: Tables,
    lines: list[LineKey],
) -> GtfsHub:
    url = server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(
        Identified([url], vehicle_positions_url=server.url("vp")),
        tmp_path / "db",
        session,
        clock=now,
    )
    await hub.async_sync(lines)
    return hub


async def test_only_shapes_of_selected_lines_are_stored(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await synced(tmp_path, feed_server, session, now, shaped(PATHS), [LINE_11])
    path = tmp_path / "db"
    assert count(path, "SELECT count(DISTINCT shape_id) FROM shape_points") == 2
    assert count(path, "SELECT count(*) FROM shape_points WHERE shape_id = 'other'") == 0
    shapes = await hub.async_line_shapes(LINE_11)
    assert [(shape.id, shape.headsign, shape.direction_id) for shape in shapes] == [
        ("east", "Beta", 0),
        ("west", "Alpha", 1),
    ]
    assert shapes[0].points == tuple(EAST)
    assert await hub.async_line_shapes(LINE_12) == []


async def test_file_without_shapes(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await synced(tmp_path, feed_server, session, now, base_tables(), [LINE_11])
    assert await hub.async_line_shapes(LINE_11) == []


async def test_added_line_brings_its_shapes_and_removed_line_takes_them(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await synced(tmp_path, feed_server, session, now, shaped(PATHS), [LINE_11])
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    assert [shape.id for shape in await hub.async_line_shapes(LINE_12)] == ["other"]
    await hub.async_sync([LINE_12])
    assert await hub.async_line_shapes(LINE_11) == []
    assert count(tmp_path / "db", "SELECT count(DISTINCT shape_id) FROM shape_points") == 1


async def test_most_common_headsign_names_a_shape(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    trips: list[Row] = [
        ("r11", "svc", "t1", 0, "east", "Short"),
        ("r11", "svc", "t3", 0, "east", "Beta"),
        ("r11", "svc", "t4", 0, "east", "Beta"),
    ]
    hub = await synced(tmp_path, feed_server, session, now, shaped(PATHS, trips), [LINE_11])
    [shape] = await hub.async_line_shapes(LINE_11)
    assert shape.headsign == "Beta"


async def test_identical_paths_under_new_ids_are_one(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    first = shaped(PATHS)
    second = shaped(
        {"east2": EAST, "west": EAST[::-1]},
        [("r11", "svc", "t1", 0, "east2", "Beta"), ("r11", "svc", "t3", 1, "west", "Alpha")],
        start="20261102",
    )
    a = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", first))
    b = feed_server.put("b.zip", write_feed(tmp_path / "b.zip", second))
    hub = GtfsHub(GenericProvider([a, b]), tmp_path / "db", session, clock=now)
    await hub.async_sync([LINE_11])
    assert count(tmp_path / "db", "SELECT count(*) FROM snapshots") == 2
    shapes = await hub.async_line_shapes(LINE_11)
    assert sorted(shape.id for shape in shapes) == ["east2", "west"]


async def test_vehicle_carries_its_shape(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    tables = shaped(PATHS)
    tables["stop_times"] = (
        tables["stop_times"][0],
        [*tables["stop_times"][1], ("t3", "09:00:00", "09:00:00", "s2", 0)],
    )
    feed_server.files["vp"] = positions(("v1", "t1", True), ("v3", "t3", True))
    hub = await synced(tmp_path, feed_server, session, now, tables, [LINE_11])
    vehicles = {str(vehicle.key): vehicle for vehicle in await hub.async_fetch_vehicles()}
    assert vehicles["v1"].shape is not None and vehicles["v1"].shape.id == "east"
    assert vehicles["v3"].shape is not None and vehicles["v3"].shape.id == "west"


async def test_vehicle_without_a_shape(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = positions(("v1", "t1", True))
    hub = await synced(tmp_path, feed_server, session, now, base_tables(), [LINE_11])
    [vehicle] = await hub.async_fetch_vehicles()
    assert vehicle.shape is None


def shape(points: list[tuple[float, float]]) -> Shape:
    return Shape(ShapeId("s"), LINE_11, None, None, tuple(points))


def test_on_a_straight_path() -> None:
    found = locate(shape(EAST), 50.0001, 19.0025)
    assert found is not None
    assert found.bearing == pytest.approx(90, abs=0.5)
    assert found.offset == pytest.approx(11, abs=1)
    assert found.along == pytest.approx(179, abs=2)


def test_on_a_bend() -> None:
    bend = [(50.0, 19.0), (50.0, 19.002), (50.002, 19.002)]
    north = locate(shape(bend), 50.0015, 19.0021)
    assert north is not None
    assert north.bearing == pytest.approx(0, abs=0.5)
    east = locate(shape(bend), 50.0, 19.001)
    assert east is not None
    assert east.bearing == pytest.approx(90, abs=0.5)


def test_far_off_the_path() -> None:
    assert locate(shape(EAST), 50.0009, 19.0025) is None


def test_too_few_points() -> None:
    assert locate(shape(EAST[:1]), 50.0, 19.0) is None


def test_out_and_back_follows_the_previous_location() -> None:
    # East along a street, north and back, then west on the same street.
    path = shape([*EAST, (50.01, 19.005), *EAST[::-1]])
    going = locate(path, 50.0, 19.0025, after=100)
    assert going is not None
    assert going.bearing == pytest.approx(90, abs=0.5)
    back = locate(path, 50.0, 19.0025, after=going.along + 2_500)
    assert back is not None
    assert back.bearing == pytest.approx(270, abs=0.5)
    assert back.along > going.along


def test_out_and_back_at_first_sight_is_not_guessed() -> None:
    path = shape([*EAST, (50.01, 19.005), *EAST[::-1]])
    assert locate(path, 50.0, 19.0025) is None


def test_far_behind_falls_back_to_the_nearest() -> None:
    found = locate(shape(EAST), 50.0, 19.0005, after=5_000)
    assert found is not None
    assert found.bearing == pytest.approx(90, abs=0.5)


def test_a_farther_pass_the_other_way_is_no_tie() -> None:
    # The way back runs 50 m north of the way out.
    path = shape([*EAST, (50.00045, 19.005), (50.00045, 19.0)])
    found = locate(path, 50.00005, 19.0025)
    assert found is not None
    assert found.bearing == pytest.approx(90, abs=0.5)


def test_the_nearer_pass_ahead_wins_over_the_first() -> None:
    path = shape([*EAST, (50.00045, 19.005), (50.00045, 19.0)])
    found = locate(path, 50.0004, 19.0025, after=10)
    assert found is not None
    assert found.bearing == pytest.approx(270, abs=0.5)
