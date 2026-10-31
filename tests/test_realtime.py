"""Joining trip updates to departures, with messages built from the protobuf classes."""

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Required, TypedDict

import pytest
from aiohttp import ClientSession
from google.transit import gtfs_realtime_pb2 as pb

from conftest import FeedServer, base_tables, write_feed
from gtfs_hub import (
    Departure,
    DepartureStatus,
    GenericProvider,
    GtfsFeedError,
    GtfsHub,
    LineKey,
    RealtimeData,
    StopKey,
)
from gtfs_hub.realtime import parse_trip_updates

LINE_11 = LineKey(3, "11")
NOW = datetime(2026, 11, 2, 6, 0, tzinfo=UTC)
STOP_2 = StopKey("s2")
TIMES = [
    ("t1", "08:00:00", "08:00:00", "s1", 3),
    ("t1", "08:10:00", "08:10:00", "s2", 7),
    ("t1", "08:20:00", "08:20:00", "s3", 9),
    ("t2", "09:10:00", "09:10:00", "s2", 0),
    ("t2", "09:20:00", "09:20:00", "s3", 1),
]


class StopSpec(TypedDict, total=False):
    sequence: int
    stop_id: str
    relationship: pb.TripUpdate.StopTimeUpdate.ScheduleRelationship
    arrival: dict[str, int]
    departure: dict[str, int]


class UpdateSpec(TypedDict, total=False):
    trip: Required[str]
    start_date: str
    trip_relationship: pb.TripDescriptor.ScheduleRelationship
    stops: list[StopSpec]


def message(*updates: UpdateSpec, stamp: datetime = NOW) -> RealtimeData:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(stamp.timestamp())
    for number, update in enumerate(updates):
        entity = feed.entity.add()
        entity.id = str(number)
        entity.trip_update.trip.trip_id = update["trip"]
        if "start_date" in update:
            entity.trip_update.trip.start_date = update["start_date"]
        if "trip_relationship" in update:
            entity.trip_update.trip.schedule_relationship = update["trip_relationship"]
        for stop in update.get("stops", ()):
            item = entity.trip_update.stop_time_update.add()
            if "sequence" in stop:
                item.stop_sequence = stop["sequence"]
            if "stop_id" in stop:
                item.stop_id = stop["stop_id"]
            if "relationship" in stop:
                item.schedule_relationship = stop["relationship"]
            _fill(item.arrival, stop.get("arrival", {}))
            _fill(item.departure, stop.get("departure", {}))
    return parse_trip_updates(feed.SerializeToString())


def _fill(event: pb.TripUpdate.StopTimeEvent, values: dict[str, int]) -> None:
    for field, value in values.items():
        setattr(event, field, value)


async def make_hub(
    tmp_path: Path,
    server: FeedServer,
    session: ClientSession,
    clock: Callable[[], datetime] = lambda: NOW,
    trip_updates_url: str | None = None,
) -> GtfsHub:
    tables = base_tables(stop_times=TIMES)
    url = server.put("a.zip", write_feed(tmp_path / "a.zip", tables))
    hub = GtfsHub(
        GenericProvider([url], trip_updates_url=trip_updates_url),
        tmp_path / "db",
        session,
        clock=clock,
    )
    await hub.async_sync([LINE_11, LineKey(3, "12")])
    return hub


async def first(hub: GtfsHub, data: RealtimeData, stop: StopKey = STOP_2) -> Departure:
    return (await hub.async_departures(stop, realtime=data, horizon=timedelta(hours=4)))[0]


async def test_fetch_from_endpoint(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(NOW.timestamp())
    feed_server.files["tu"] = feed.SerializeToString()
    hub = await make_hub(tmp_path, feed_server, session, trip_updates_url=feed_server.url("tu"))
    data = await hub.async_fetch_realtime()
    assert data is not None
    assert data.timestamp == NOW
    feed_server.files["tu"] = b"\xff\xff not protobuf"
    with pytest.raises(GtfsFeedError):
        await hub.async_fetch_realtime()


async def test_no_endpoint_gives_none(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    assert await hub.async_fetch_realtime() is None


async def test_trip_without_update_stays_scheduled(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    departure = await first(hub, message({"trip": "other", "stops": [{"sequence": 1}]}))
    assert departure.status is DepartureStatus.SCHEDULED
    assert departure.delay is None
    assert departure.predicted is None


async def test_delay_propagates_to_later_stops(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"sequence": 3, "arrival": {"delay": 120}}]})
    departure = await first(hub, data)
    assert departure.delay == 120
    assert departure.status is DepartureStatus.PREDICTED
    assert departure.predicted == departure.scheduled + timedelta(seconds=120)


async def test_negative_delay(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"sequence": 7, "arrival": {"delay": -60}}]})
    assert (await first(hub, data)).delay == -60


async def test_time_wins_and_departure_wins(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    moment = datetime(2026, 11, 2, 7, 15, tzinfo=UTC)
    data = message(
        {
            "trip": "t1",
            "stops": [
                {
                    "sequence": 7,
                    "arrival": {"delay": 1},
                    "departure": {"delay": 999, "time": int(moment.timestamp())},
                }
            ],
        }
    )
    departure = await first(hub, data)
    assert departure.predicted == moment
    assert departure.delay == 300


async def test_match_by_stop_id(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"stop_id": "s1", "arrival": {"delay": 30}}]})
    assert (await first(hub, data)).delay == 30


async def test_passed_stop_gives_no_departure(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"sequence": 9, "arrival": {"delay": 0}}]})
    departures = await hub.async_departures(
        StopKey("s2"), realtime=data, horizon=timedelta(hours=4)
    )
    assert [d.trip_id for d in departures] == ["t2"]


async def test_late_vehicle_stays_after_its_scheduled_time(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    stamp = datetime(2026, 11, 2, 7, 12, tzinfo=UTC)
    hub = await make_hub(tmp_path, feed_server, session, lambda: stamp)
    data = message(
        {"trip": "t1", "stops": [{"sequence": 7, "arrival": {"delay": 300}}]}, stamp=stamp
    )
    departure = await first(hub, data)
    assert departure.trip_id == "t1"
    assert departure.predicted == datetime(2026, 11, 2, 7, 15, tzinfo=UTC)


async def test_cancelled_trip_stays_listed(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    for relationship in (pb.TripDescriptor.CANCELED, pb.TripDescriptor.DELETED):
        departure = await first(hub, message({"trip": "t1", "trip_relationship": relationship}))
        assert departure.status is DepartureStatus.CANCELLED


async def test_skipped_stop(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    skipped = pb.TripUpdate.StopTimeUpdate.SKIPPED
    data = message({"trip": "t1", "stops": [{"sequence": 7, "relationship": skipped}]})
    assert (await first(hub, data)).status is DepartureStatus.SKIPPED


async def test_no_data_keeps_the_schedule(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    no_data = pb.TripUpdate.StopTimeUpdate.NO_DATA
    data = message({"trip": "t1", "stops": [{"sequence": 3, "relationship": no_data}]})
    departure = await first(hub, data)
    assert departure.status is DepartureStatus.SCHEDULED
    assert departure.delay is None


async def test_nearest_service_day_gets_the_prediction(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"sequence": 3, "arrival": {"delay": 60}}]})
    departures = await hub.async_departures(
        STOP_2, realtime=data, horizon=timedelta(hours=30), limit=9
    )
    today = [d for d in departures if d.trip_id == "t1" and d.service_date == date(2026, 11, 2)]
    tomorrow = [d for d in departures if d.trip_id == "t1" and d.service_date == date(2026, 11, 3)]
    assert today[0].delay == 60
    assert tomorrow[0].delay is None


async def test_start_date_decides_the_service_day(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message(
        {
            "trip": "t1",
            "start_date": "20261103",
            "stops": [{"sequence": 3, "arrival": {"delay": 60}}],
        }
    )
    departures = await hub.async_departures(
        STOP_2, realtime=data, horizon=timedelta(hours=30), limit=9
    )
    delays = {d.service_date: d.delay for d in departures if d.trip_id == "t1"}
    assert delays == {date(2026, 11, 2): None, date(2026, 11, 3): 60}


async def test_stale_data_is_dropped(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    old = NOW - timedelta(minutes=6)
    data = message({"trip": "t1", "stops": [{"sequence": 3, "arrival": {"delay": 60}}]}, stamp=old)
    assert data.is_stale(NOW, timedelta(minutes=5))
    assert not data.is_stale(NOW, timedelta(minutes=7))
    assert (await first(hub, data)).status is DepartureStatus.SCHEDULED


def test_message_without_timestamp_is_stale() -> None:
    assert RealtimeData(None).is_stale(NOW, timedelta(minutes=5))


def test_header_timestamp_is_utc() -> None:
    assert message().timestamp == NOW


async def test_unplaceable_update_keeps_the_departure(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    data = message({"trip": "t1", "stops": [{"stop_id": "elsewhere", "arrival": {"delay": 30}}]})
    departure = await first(hub, data)
    assert departure.trip_id == "t1"
    assert departure.status is DepartureStatus.SCHEDULED


async def test_delay_propagates_past_a_skipped_stop(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession
) -> None:
    hub = await make_hub(tmp_path, feed_server, session)
    skipped = pb.TripUpdate.StopTimeUpdate.SKIPPED
    data = message(
        {
            "trip": "t1",
            "stops": [
                {"sequence": 3, "arrival": {"delay": 300}},
                {"sequence": 5, "relationship": skipped},
            ],
        }
    )
    assert (await first(hub, data)).delay == 300


def test_bad_start_date_does_not_sink_the_feed() -> None:
    data = message({"trip": "t1", "start_date": "2026-11-02"})
    assert data.trips["t1"].start_date is None
