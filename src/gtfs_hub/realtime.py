"""Parse trip updates. Every field stays optional, as feeds fill them in differently."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Final

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from .errors import GtfsFeedError

# By name, as older bindings, such as those Home Assistant pins, have no DELETED member.
_CANCELLED: Final = frozenset({"CANCELED", "DELETED"})


class StopRelationship(StrEnum):
    """What a stop update says about its stop."""

    SCHEDULED = "scheduled"
    SKIPPED = "skipped"
    NO_DATA = "no_data"


@dataclass(frozen=True, slots=True)
class StopUpdate:
    """One stop_time_update, preferring departure values over arrival values."""

    sequence: int | None
    stop_id: str | None
    delay: int | None
    time: datetime | None
    relationship: StopRelationship


@dataclass(frozen=True, slots=True)
class TripUpdate:
    """Live data of one trip."""

    trip_id: str
    start_date: date | None
    cancelled: bool
    stops: tuple[StopUpdate, ...]


@dataclass(frozen=True, slots=True)
class RealtimeData:
    """One parsed message of trip updates."""

    timestamp: datetime | None
    trips: Mapping[str, TripUpdate] = field(default_factory=dict, hash=False)

    def is_stale(self, now: datetime, max_age: timedelta) -> bool:
        """Tell whether the message is too old to trust. No timestamp proves nothing."""
        return self.timestamp is None or now - self.timestamp > max_age


def _event(update: gtfs_realtime_pb2.TripUpdate.StopTimeUpdate) -> tuple[int | None, int | None]:
    for name in ("departure", "arrival"):
        if update.HasField(name):
            event = getattr(update, name)
            delay = event.delay if event.HasField("delay") else None
            time = event.time if event.HasField("time") else None
            if delay is not None or time is not None:
                return delay, time
    return None, None


def _stop_update(update: gtfs_realtime_pb2.TripUpdate.StopTimeUpdate) -> StopUpdate:
    delay, time = _event(update)
    kinds = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
    relationship = StopRelationship.SCHEDULED
    if update.schedule_relationship == kinds.SKIPPED:
        relationship = StopRelationship.SKIPPED
    elif update.schedule_relationship == kinds.NO_DATA:
        relationship = StopRelationship.NO_DATA
    return StopUpdate(
        sequence=update.stop_sequence if update.HasField("stop_sequence") else None,
        stop_id=update.stop_id if update.HasField("stop_id") else None,
        delay=delay,
        time=datetime.fromtimestamp(time, UTC) if time else None,
        relationship=relationship,
    )


def _parse_start_date(value: str) -> date | None:
    """Return a start date. A bad one is as good as none: one trip must not sink the feed."""
    try:
        return datetime.strptime(value, "%Y%m%d").date() if value else None
    except ValueError:
        return None


def parse_trip_updates(body: bytes) -> RealtimeData:
    """Parse a protobuf message of trip updates. Blocks on large messages."""
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(body)
    except DecodeError as err:
        raise GtfsFeedError(f"Corrupt realtime message: {err}") from err
    relationships = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
    trips = {}
    for entity in message.entity:
        if not entity.HasField("trip_update") or not entity.trip_update.trip.trip_id:
            continue
        update = entity.trip_update
        trips[update.trip.trip_id] = TripUpdate(
            trip_id=update.trip.trip_id,
            start_date=_parse_start_date(update.trip.start_date),
            cancelled=relationships.Name(update.trip.schedule_relationship) in _CANCELLED,
            stops=tuple(_stop_update(stop) for stop in update.stop_time_update),
        )
    stamp = message.header.timestamp
    return RealtimeData(datetime.fromtimestamp(stamp, UTC) if stamp else None, trips)
