"""Departures of a stop, for every service date reaching the period asked for. Blocks."""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .catalogue import read_snapshots
from .errors import GtfsNoDataError
from .models import Departure, DepartureStatus, LineKey, Snapshot
from .realtime import RealtimeData, StopRelationship, StopUpdate, TripUpdate
from .snapshots import DAY_SECONDS, select_snapshot

# Read this far back, so a late vehicle still shows as a future departure.
_LOOK_BACK = timedelta(hours=1)


def service_instant(day: date, seconds: int, zone: ZoneInfo) -> datetime:
    """Return the instant of a schedule time: noon of the day, minus 12 hours, plus the time."""
    noon = datetime.combine(day, time(12), zone).astimezone(UTC)
    return noon + timedelta(seconds=seconds - 12 * 3_600)


def running_services(conn: sqlite3.Connection, snapshot: int, day: date) -> set[str]:
    """Return the services running on a date, after calendar exceptions."""
    iso = day.isoformat()
    running = {
        service_id
        for service_id, weekdays in conn.execute(
            "SELECT service_id, weekdays FROM services"
            " WHERE snapshot = ? AND start_date <= ? AND end_date >= ?",
            (snapshot, iso, iso),
        )
        if weekdays[day.weekday()] == "1"
    }
    for service_id, exception_type in conn.execute(
        "SELECT service_id, exception_type FROM service_exceptions WHERE snapshot = ? AND day = ?",
        (snapshot, iso),
    ):
        if exception_type == 1:
            running.add(service_id)
        elif exception_type == 2:
            running.discard(service_id)
    return running


def _zone(conn: sqlite3.Connection, snapshot: int) -> ZoneInfo:
    name = conn.execute("SELECT timezone FROM snapshots WHERE id = ?", (snapshot,)).fetchone()[0]
    try:
        return ZoneInfo(name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


@dataclass(frozen=True, slots=True)
class _Instance:
    """One trip on one service date."""

    snapshot: Snapshot
    day: date
    zone: ZoneInfo
    services: frozenset[str]


class _Query:
    """State of one request, so each service date is worked out once."""

    def __init__(self, conn: sqlite3.Connection, now: datetime) -> None:
        self.conn = conn
        self.now = now
        self.snapshots = read_snapshots(conn)
        self._instances: dict[date, _Instance | None] = {}
        self._live_days: dict[str, date | None] = {}

    def instance(self, day: date) -> _Instance | None:
        if day not in self._instances:
            snapshot = select_snapshot(self.snapshots, day)
            self._instances[day] = (
                None
                if snapshot is None
                else _Instance(
                    snapshot,
                    day,
                    _zone(self.conn, snapshot.id),
                    frozenset(running_services(self.conn, snapshot.id, day)),
                )
            )
        return self._instances[day]

    def service_dates(self, horizon: timedelta) -> list[date]:
        row = self.conn.execute("SELECT max(max_seconds) FROM snapshots").fetchone()
        reach = (row[0] or DAY_SECONDS) // DAY_SECONDS
        first = self.now.date() - timedelta(days=reach + 1)
        last = (self.now + horizon).date() + timedelta(days=1)
        return [first + timedelta(days=n) for n in range((last - first).days + 1)]

    def live_day(self, trip: TripUpdate, days: list[date]) -> date | None:
        """Return the service date an update without a start date belongs to."""
        if trip.trip_id not in self._live_days:
            best: tuple[timedelta, date] | None = None
            for day in days:
                span = self._span(trip.trip_id, day)
                if span is None:
                    continue
                start, end = span
                distance = max(start - self.now, self.now - end, timedelta(0))
                if best is None or distance < best[0]:
                    best = (distance, day)
            self._live_days[trip.trip_id] = best[1] if best else None
        return self._live_days[trip.trip_id]

    def _span(self, trip_id: str, day: date) -> tuple[datetime, datetime] | None:
        instance = self.instance(day)
        if instance is None:
            return None
        row = self.conn.execute(
            "SELECT t.service_id, min(st.departure_seconds), max(st.arrival_seconds)"
            " FROM trips t JOIN stop_times st"
            " ON st.snapshot = t.snapshot AND st.trip_id = t.trip_id"
            " WHERE t.snapshot = ? AND t.trip_id = ?",
            (instance.snapshot.id, trip_id),
        ).fetchone()
        if row[0] not in instance.services or row[1] is None or row[2] is None:
            return None
        return (
            service_instant(day, row[1], instance.zone),
            service_instant(day, row[2], instance.zone),
        )

    def scheduled_at(self, instance: _Instance, trip_id: str, sequence: int) -> int | None:
        row = self.conn.execute(
            "SELECT departure_seconds FROM stop_times"
            " WHERE snapshot = ? AND trip_id = ? AND stop_sequence = ?",
            (instance.snapshot.id, trip_id, sequence),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def sequence_of(self, instance: _Instance, trip_id: str, update: StopUpdate) -> int | None:
        if update.sequence is not None:
            return update.sequence
        row = self.conn.execute(
            "SELECT min(stop_sequence) FROM stop_times"
            " WHERE snapshot = ? AND trip_id = ? AND stop_id = ?",
            (instance.snapshot.id, trip_id, update.stop_id),
        ).fetchone()
        return None if row[0] is None else int(row[0])


def _prediction(
    query: _Query, instance: _Instance, trip: TripUpdate, sequence: int
) -> tuple[bool, int | None, DepartureStatus]:
    """Return whether the stop is passed, the delay, and the status."""
    latest: tuple[int, StopUpdate] | None = None
    for update in trip.stops:
        position = query.sequence_of(instance, trip.trip_id, update)
        if (
            position is not None
            and position <= sequence
            and (latest is None or position > latest[0])
        ):
            latest = (position, update)
    if latest is None:
        return bool(trip.stops), None, DepartureStatus.SCHEDULED
    position, update = latest
    if update.relationship is StopRelationship.SKIPPED:
        status = DepartureStatus.SKIPPED if position == sequence else DepartureStatus.SCHEDULED
        return False, None, status
    if update.relationship is StopRelationship.NO_DATA:
        return False, None, DepartureStatus.SCHEDULED
    if update.time is not None:
        scheduled = query.scheduled_at(instance, trip.trip_id, position)
        if scheduled is None:
            return False, None, DepartureStatus.SCHEDULED
        moment = service_instant(instance.day, scheduled, instance.zone)
        return False, int((update.time - moment).total_seconds()), DepartureStatus.PREDICTED
    if update.delay is None:
        return False, None, DepartureStatus.SCHEDULED
    return False, update.delay, DepartureStatus.PREDICTED


def list_departures(
    conn: sqlite3.Connection,
    stop_key: str,
    now: datetime,
    realtime: RealtimeData | None,
    max_age: timedelta,
    limit: int,
    horizon: timedelta,
) -> list[Departure]:
    """Return upcoming departures of a stop, soonest first."""
    query = _Query(conn, now)
    if not query.snapshots:
        raise GtfsNoDataError("No schedule is stored")
    today = now.astimezone(_zone(conn, query.snapshots[-1].id)).date()
    if select_snapshot(query.snapshots, today) is None:
        raise GtfsNoDataError(f"No schedule covers {today}")
    fresh = realtime if realtime and not realtime.is_stale(now, max_age) else None
    days = query.service_dates(horizon)
    found: list[Departure] = []
    for day in days:
        instance = query.instance(day)
        if instance is not None:
            found += _departures_on(query, instance, stop_key, fresh, days, horizon)
    found.sort(key=lambda departure: (departure.expected, departure.trip_id))
    return found[:limit]


def _departures_on(
    query: _Query,
    instance: _Instance,
    stop_key: str,
    realtime: RealtimeData | None,
    days: list[date],
    horizon: timedelta,
) -> list[Departure]:
    start = service_instant(instance.day, 0, instance.zone)
    earliest = int((query.now - _LOOK_BACK - start).total_seconds())
    latest = int((query.now + horizon - start).total_seconds())
    rows = query.conn.execute(
        """
        SELECT st.trip_id, st.stop_sequence, st.departure_seconds,
            coalesce(st.headsign, t.headsign), l.route_type, l.short_name, t.service_id,
            (SELECT max(last.stop_sequence) FROM stop_times last
             WHERE last.snapshot = st.snapshot AND last.trip_id = st.trip_id)
        FROM stops s
        JOIN stop_times st ON st.snapshot = s.snapshot AND st.stop_id = s.stop_id
        JOIN trips t ON t.snapshot = st.snapshot AND t.trip_id = st.trip_id
        JOIN lines l ON l.snapshot = t.snapshot AND l.route_id = t.route_id
        WHERE s.snapshot = ? AND s.stop_key = ? AND st.pickup_type != 1
            AND st.departure_seconds BETWEEN ? AND ?
        """,
        (instance.snapshot.id, stop_key, earliest, latest),
    ).fetchall()
    found = []
    for trip_id, sequence, seconds, headsign, route_type, short_name, service, last in rows:
        if service not in instance.services or sequence == last:
            continue
        scheduled = service_instant(instance.day, seconds, instance.zone)
        delay = None
        status = DepartureStatus.SCHEDULED
        trip = realtime.trips.get(trip_id) if realtime else None
        if trip is not None:
            live_day = trip.start_date or query.live_day(trip, days)
            if live_day == instance.day:
                if trip.cancelled:
                    status = DepartureStatus.CANCELLED
                else:
                    passed, delay, status = _prediction(query, instance, trip, sequence)
                    if passed:
                        continue
        predicted = scheduled + timedelta(seconds=delay) if delay is not None else None
        if (predicted or scheduled) < query.now:
            continue
        found.append(
            Departure(
                stop_key,
                LineKey(route_type, short_name),
                headsign,
                scheduled,
                predicted,
                delay,
                status,
                trip_id,
                instance.day,
            )
        )
    return found
