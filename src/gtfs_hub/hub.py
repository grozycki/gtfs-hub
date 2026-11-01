"""The public entry point: keep a local schedule and query it."""

import asyncio
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Callable, Collection
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiohttp import ClientError, ClientSession, ClientTimeout

from . import catalogue, importer
from .departures import list_departures
from .download import async_download, raise_for_status
from .errors import GtfsConnectionError, GtfsError, GtfsFeedError
from .models import (
    Departure,
    Line,
    LineKey,
    ResolvedId,
    Snapshot,
    StaticFeedRef,
    Stop,
    StopChoice,
    StopKey,
    SyncResult,
    TripId,
    Vehicle,
    VehicleKey,
)
from .provider import Provider
from .realtime import RealtimeData, parse_trip_updates, parse_vehicle_positions
from .schema import SCHEMA_VERSION, open_read, open_work, stored_version
from .snapshots import is_expired, missing_dates, select_snapshot

# Far below the session default, as a stalled feed must not hold up the schedule.
_REALTIME_TIMEOUT = ClientTimeout(total=20)
_NO_FIX = 0.001


def _is_real_position(latitude: float, longitude: float) -> bool:
    """Tell a position from the 0,0 feeds send when a vehicle has no GPS fix."""
    in_range = abs(latitude) <= 90 and abs(longitude) <= 180
    return in_range and not (abs(latitude) < _NO_FIX and abs(longitude) < _NO_FIX)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _local(now: datetime, timezone: str | None) -> datetime:
    try:
        return now.astimezone(ZoneInfo(timezone)) if timezone else now
    except ZoneInfoNotFoundError:
        return now


def _with_conn[T](
    opener: Callable[[Path], sqlite3.Connection],
    path: Path,
    job: Callable[[sqlite3.Connection], T],
) -> T:
    """Run one job on its own connection, as connections belong to one thread."""
    conn = opener(path)
    try:
        return job(conn)
    finally:
        conn.close()


def _count(result: SyncResult, action: str) -> SyncResult:
    if action == "imported":
        return replace(result, imported=result.imported + 1)
    if action == "relabelled":
        return replace(result, relabelled=result.relabelled + 1)
    if action == "extended":
        return replace(result, extended=result.extended + 1)
    return replace(result, skipped=result.skipped + 1)


def _swap_in(work_path: Path, path: Path) -> None:
    """Check a finished database and put it in place in one step."""
    status = _with_conn(
        open_work, work_path, lambda conn: conn.execute("PRAGMA quick_check").fetchone()[0]
    )
    if status != "ok":
        raise GtfsFeedError(f"Integrity check failed: {status}")
    descriptor = os.open(work_path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(work_path, path)


class GtfsHub:
    """A local schedule of one provider, sized by the selected lines.

    The database is a cache: it can be deleted at any time, and the next sync rebuilds it.
    """

    def __init__(
        self,
        provider: Provider,
        path: Path,
        session: ClientSession,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._provider = provider
        self._path = Path(os.path.abspath(path))
        self._session = session
        self._clock = clock
        self._lock = asyncio.Lock()
        self._trips: dict[str, catalogue.TripInfo] | None = None

    @property
    def provider(self) -> Provider:
        """Return the source of this schedule."""
        return self._provider

    async def _run[T](self, job: Callable[[], T]) -> T:
        return await asyncio.get_running_loop().run_in_executor(None, job)

    async def async_sync(self, selection: Collection[LineKey]) -> SyncResult:
        """Bring the database up to date with the source and the selection.

        Failure leaves the previous database untouched and raises a GtfsError.
        """
        async with self._lock:
            refs = await self._provider.async_list_static_feeds(self._session)
            if not refs:
                raise GtfsFeedError(f"{self._provider.key} listed no schedule files")
            workspace = Path(await self._run(self._make_workspace))
            try:
                return await self._sync_in(workspace, refs, selection)
            except (OSError, sqlite3.Error) as err:
                raise GtfsError(f"Cannot store the schedule: {err}") from err
            finally:
                await self._run(partial(shutil.rmtree, workspace, ignore_errors=True))

    def _make_workspace(self) -> str:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        prefix = f".{self._path.name}."
        for leftover in self._path.parent.glob(f"{prefix}*"):
            shutil.rmtree(leftover, ignore_errors=True)
        return tempfile.mkdtemp(prefix=prefix, dir=self._path.parent)

    def _work[T](self, work_path: Path, job: Callable[[sqlite3.Connection], T]) -> T:
        return _with_conn(open_work, work_path, job)

    async def _sync_in(
        self, workspace: Path, refs: list[StaticFeedRef], selection: Collection[LineKey]
    ) -> SyncResult:
        work_path = workspace / "work.db"
        await self._run(partial(self._prepare, work_path))
        result = SyncResult()
        listed: set[int] = set()
        for number, ref in enumerate(refs):
            snapshot, download = await self._run(
                partial(self._work, work_path, partial(_needs_download, ref, selection))
            )
            if not download:
                assert snapshot is not None
                listed.add(snapshot)
                result = replace(result, skipped=result.skipped + 1)
                continue
            file_path = workspace / f"{number}.zip"
            await async_download(self._session, ref.url, file_path)
            outcome = await self._run(
                partial(
                    self._work,
                    work_path,
                    partial(_apply, ref, file_path, selection, self._provider),
                )
            )
            await self._run(file_path.unlink)
            listed.add(outcome.snapshot_id)
            result = _count(result, outcome.action)
        removed, lines_removed = await self._run(
            partial(self._work, work_path, partial(self._finish, listed, selection))
        )
        if removed or lines_removed or result.imported or result.relabelled or result.extended:
            await self._run(partial(_swap_in, work_path, self._path))
            self._trips = None
        return replace(result, removed=removed)

    def _prepare(self, work_path: Path) -> None:
        if stored_version(self._path) == SCHEMA_VERSION:
            shutil.copy2(self._path, work_path)
        open_work(work_path).close()

    def _finish(
        self, listed: set[int], selection: Collection[LineKey], conn: sqlite3.Connection
    ) -> tuple[int, bool]:
        lines_removed = importer.remove_deselected(conn, selection)
        snapshots = catalogue.read_snapshots(conn)
        details = {
            row[0]: (row[1], row[2])
            for row in conn.execute("SELECT id, timezone, max_seconds FROM snapshots")
        }
        now = self._clock()
        expired = {
            snapshot.id
            for snapshot in snapshots
            if is_expired(
                snapshot.valid_to,
                details[snapshot.id][1],
                _local(now, details[snapshot.id][0]),
            )
        }
        windows = [snapshot for snapshot in snapshots if snapshot.id in listed]
        # A file gone from the listing stays until newer files cover what is left of its window.
        superseded = set()
        for snapshot in snapshots:
            if snapshot.id in listed or snapshot.id in expired:
                continue
            start = max(snapshot.valid_from, _local(now, details[snapshot.id][0]).date())
            # With its window over, a kept snapshot is serving night trips, so nothing replaces it.
            if start <= snapshot.valid_to and not missing_dates(windows, start, snapshot.valid_to):
                superseded.add(snapshot.id)
        removed = expired | superseded
        importer.delete_snapshots(conn, removed)
        if removed or lines_removed:
            conn.execute("VACUUM")
        return len(removed), lines_removed

    async def _read[T](self, job: Callable[[sqlite3.Connection], T]) -> T:
        return await self._run(partial(_with_conn, open_read, self._path, job))

    async def _async_fetch_message(self, url: str) -> bytes:
        headers = self._provider.realtime_endpoints().headers
        try:
            async with self._session.get(
                url, headers=headers, timeout=_REALTIME_TIMEOUT
            ) as response:
                raise_for_status(response, url)
                return await response.read()
        except (ClientError, TimeoutError) as err:
            raise GtfsConnectionError(f"Cannot fetch {url}: {err}") from err

    async def async_fetch_realtime(self) -> RealtimeData | None:
        """Fetch trip updates, or return None when the source has no such endpoint.

        Poll this once and pass the result to each departures call.
        """
        url = self._provider.realtime_endpoints().trip_updates
        if url is None:
            return None
        body = await self._async_fetch_message(url)
        return await self._run(partial(parse_trip_updates, body))

    async def async_fetch_vehicles(self) -> list[Vehicle]:
        """Fetch vehicles of the stored lines. Those the provider cannot identify are left out."""
        url = self._provider.realtime_endpoints().vehicle_positions
        if url is None:
            return []
        body = await self._async_fetch_message(url)
        positions = await self._run(partial(parse_vehicle_positions, body))
        if self._trips is None:
            self._trips = await self._read(catalogue.trip_lines)
        trips = self._trips
        vehicles = []
        for position in positions:
            trip = trips.get(position.trip_id)
            key = self._provider.vehicle_key(position.vehicle_id, position.label, position.trip_id)
            if (
                trip is not None
                and key is not None
                and position.latitude is not None
                and position.longitude is not None
                and _is_real_position(position.latitude, position.longitude)
            ):
                vehicles.append(
                    Vehicle(
                        VehicleKey(key),
                        trip.line,
                        TripId(position.trip_id),
                        trip.headsign,
                        position.latitude,
                        position.longitude,
                        position.measured,
                        trip.planned_class,
                    )
                )
        return vehicles

    async def async_departures(
        self,
        stop_key: StopKey,
        *,
        realtime: RealtimeData | None = None,
        limit: int = 10,
        horizon: timedelta = timedelta(hours=12),
        max_age: timedelta = timedelta(minutes=5),
    ) -> list[Departure]:
        """Return upcoming departures of a stop for the stored lines, soonest first.

        Realtime older than max_age is ignored, so the schedule still answers.
        """
        now = self._clock()
        return await self._read(
            lambda conn: list_departures(conn, stop_key, now, realtime, max_age, limit, horizon)
        )

    async def async_snapshots(self) -> list[Snapshot]:
        """Return the stored snapshots."""
        return await self._read(catalogue.read_snapshots)

    async def async_snapshot_for(self, service_date: date) -> Snapshot | None:
        """Return the snapshot to use for a service date."""
        return select_snapshot(await self.async_snapshots(), service_date)

    async def async_missing_dates(self, start: date, end: date) -> list[date]:
        """Return the dates of a range no snapshot covers."""
        return missing_dates(await self.async_snapshots(), start, end)

    async def async_list_lines(self) -> list[Line]:
        """Return every line, in natural order."""
        return await self._read(catalogue.list_lines)

    async def async_stop_choices(self, lines: Collection[LineKey]) -> list[StopChoice]:
        """Return the stops of stored lines, with what tells them apart."""
        return await self._read(lambda conn: catalogue.stop_choices(conn, lines))

    async def async_search_lines(self, query: str, limit: int = 20) -> list[Line]:
        """Find lines by short or long name."""
        return await self._read(lambda conn: catalogue.search_lines(conn, query, limit))

    async def async_search_stops(
        self, query: str, limit: int = 20, only: Collection[StopKey] | None = None
    ) -> list[Stop]:
        """Find stops by name or code, optionally among the given stop keys."""
        return await self._read(lambda conn: catalogue.search_stops(conn, query, limit, only))

    async def async_resolve_line(self, key: LineKey) -> list[ResolvedId]:
        """Return the route IDs of a line. Empty means it is absent."""
        return await self._read(lambda conn: catalogue.resolve_line(conn, key))

    async def async_resolve_stop(self, key: StopKey) -> list[ResolvedId]:
        """Return the stop IDs of a stop. Empty means it is absent."""
        return await self._read(lambda conn: catalogue.resolve_stop(conn, key))

    async def async_stops_of_line(self, key: LineKey) -> list[Stop]:
        """Return the stops of a selected line across current snapshots."""
        return await self._read(lambda conn: catalogue.stops_of_line(conn, key))


def _needs_download(
    ref: StaticFeedRef, selection: Collection[LineKey], conn: sqlite3.Connection
) -> tuple[int | None, bool]:
    return importer.needs_download(conn, ref, selection)


def _apply(
    ref: StaticFeedRef,
    path: Path,
    selection: Collection[LineKey],
    provider: Provider,
    conn: sqlite3.Connection,
) -> importer.FileOutcome:
    return importer.apply_file(conn, ref, path, selection, provider)
