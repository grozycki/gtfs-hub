"""Vehicle positions, with messages built from the protobuf classes."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import override
from unittest.mock import patch

import pytest
from aiohttp import ClientSession
from google.transit import gtfs_realtime_pb2 as pb

from conftest import FeedServer, base_tables, write_feed
from gtfs_hub import GenericProvider, GtfsFeedError, GtfsHub, LineKey

LINE_11 = LineKey(3, "11")


class Identified(GenericProvider):
    @override
    def vehicle_key(self, vehicle_id: str, label: str, trip_id: str) -> str | None:
        return vehicle_id or None


def positions(*vehicles: tuple[str, str, bool]) -> bytes:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for number, (vehicle_id, trip_id, located) in enumerate(vehicles):
        entity = feed.entity.add()
        entity.id = str(number)
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.trip.trip_id = trip_id
        entity.vehicle.timestamp = 1_800_000_000
        if located:
            entity.vehicle.position.latitude = 50.5
            entity.vehicle.position.longitude = 19.5
    return feed.SerializeToString()


async def make_hub(
    tmp_path: Path,
    server: FeedServer,
    session: ClientSession,
    now: Callable[[], datetime],
    provider: type[GenericProvider] = Identified,
    realtime: bool = True,
) -> GtfsHub:
    url = server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    endpoint = server.url("vp") if realtime else None
    hub = GtfsHub(
        provider([url], vehicle_positions_url=endpoint), tmp_path / "db", session, clock=now
    )
    await hub.async_sync([LINE_11])
    return hub


async def test_selected_line_only(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = positions(("v1", "t1", True), ("v2", "t2", True), ("v3", "x", True))
    hub = await make_hub(tmp_path, feed_server, session, now)
    [vehicle] = await hub.async_fetch_vehicles()
    assert vehicle.key == "v1"
    assert vehicle.line == LINE_11
    assert vehicle.latitude == pytest.approx(50.5)
    assert vehicle.measured == datetime.fromtimestamp(1_800_000_000, UTC)


async def test_no_key_or_position_is_left_out(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = positions(("", "t1", True), ("v2", "t1", False))
    hub = await make_hub(tmp_path, feed_server, session, now)
    assert await hub.async_fetch_vehicles() == []


async def test_provider_without_identity(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = positions(("v1", "t1", True))
    hub = await make_hub(tmp_path, feed_server, session, now, GenericProvider)
    assert await hub.async_fetch_vehicles() == []


async def test_no_endpoint(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    hub = await make_hub(tmp_path, feed_server, session, now, realtime=False)
    assert await hub.async_fetch_vehicles() == []


async def test_corrupt_message(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = b"\xff\xff not protobuf"
    hub = await make_hub(tmp_path, feed_server, session, now)
    with pytest.raises(GtfsFeedError):
        await hub.async_fetch_vehicles()


async def test_stored_trips_are_read_once_per_sync(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    feed_server.files["vp"] = positions(("v1", "t1", True), ("v2", "t2", True))
    hub = await make_hub(tmp_path, feed_server, session, now)
    assert len(await hub.async_fetch_vehicles()) == 1
    with patch("gtfs_hub.hub.catalogue.trip_lines") as read:
        await hub.async_fetch_vehicles()
    read.assert_not_called()
    await hub.async_sync([LINE_11, LineKey(3, "12")])
    assert len(await hub.async_fetch_vehicles()) == 2
