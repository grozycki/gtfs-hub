"""The public entry point: keep a local schedule and query it."""

import asyncio
import hashlib
import logging
import os
import shutil
import sqlite3
import tempfile
import zipfile
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
    Shape,
    Snapshot,
    StaticFeedRef,
    Stop,
    StopChoice,
    StopKey,
    SyncResult,
    TripDelay,
    TripId,
    Vehicle,
    VehicleKey,
)
from .provider import Provider
from .realtime import (
    RealtimeData,
    StopRelationship,
    TripUpdate,
    parse_trip_updates,
    parse_vehicle_positions,
)
from .schema import SCHEMA_VERSION, open_read, open_work, stored_version
from .snapshots import is_expired, missing_dates, select_snapshot

# Far below the session default, as a stalled feed must not hold up the schedule.
_REALTIME_TIMEOUT = ClientTimeout(total=20)
_NO_FIX = 0.001
_LOGGER = logging.getLogger(__name__)


def _is_real_position(latitude: float, longitude: float) -> bool:
    """Tell a position from the 0,0 feeds send when a vehicle has no GPS fix."""
    in_range = abs(latitude) <= 90 and abs(longitude) <= 180
    return in_range and not (abs(latitude) < _NO_FIX and abs(longitude) < _NO_FIX)


def _trip_delay(update: TripUpdate) -> int | None:
    """Return the delay of the first stop update that states one, as feeds list from the next."""
    if update.cancelled:
        return None
    return next(
        (
            stop.delay
            for stop in update.stops
            if stop.delay is not None and stop.relationship != StopRelationship.SKIPPED
        ),
        None,
    )


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


def _keep(file_path: Path, kept: Path) -> None:
    """Move an imported download among the kept files, or drop it when that fails.

    Keeping only saves a later download, so a failure must not fail the sync.
    """
    try:
        kept.parent.mkdir(exist_ok=True)
        if kept.is_dir() and not kept.is_symlink():
            shutil.rmtree(kept)
        os.replace(file_path, kept)
    except OSError as err:
        _LOGGER.warning("Cannot keep %s for later syncs: %s", kept.name, err)
        file_path.unlink(missing_ok=True)


def _swap_in(work_path: Path, path: Path) -> None:
    """Refresh planner statistics, check a finished database and put it in place in one step."""
    # Without statistics the planner picks indexes by guesswork, which grows costly with the data.
    _with_conn(open_work, work_path, lambda conn: conn.execute("ANALYZE"))
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


def kept_files_dir(path: Path) -> Path:
    """Return where the schedule files of a database are kept between syncs."""
    return path.with_name(f"{path.name}.files")


def _kept_name(ref: StaticFeedRef) -> str | None:
    """Name a file by its URL and version, or None when nothing tells a changed file apart."""
    if ref.version is None:
        return None
    digest = hashlib.sha256(f"{ref.url}\n{ref.version}".encode()).hexdigest()
    return f"{digest[:24]}.zip"


def _is_readable_zip(path: Path) -> bool:
    """Tell whether a kept file opens as an archive, which a cut-off file does not.

    Its members are checked as they are read, and a failing file is dropped then.
    """
    try:
        with zipfile.ZipFile(path):
            return True
    except (OSError, zipfile.BadZipFile):
        return False


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _prune_kept(directory: Path, wanted: set[str]) -> None:
    """Delete kept files the listing no longer names, and anything else left there."""
    if not directory.is_dir():
        return
    for entry in directory.iterdir():
        if entry.name not in wanted:
            _remove(entry)


def delete_store(path: Path) -> None:
    """Delete a schedule database, its kept files and leftovers of interrupted syncs. Blocks."""
    path = Path(os.path.abspath(path))
    path.unlink(missing_ok=True)
    _remove(kept_files_dir(path))
    if path.parent.is_dir():
        for leftover in path.parent.glob(f".{path.name}.*"):
            _remove(leftover)


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
            wanted = {name for ref in refs if (name := _kept_name(ref)) is not None}
            try:
                workspace = Path(await self._run(self._make_workspace))
                try:
                    return await self._sync_in(workspace, refs, selection)
                except (OSError, sqlite3.Error) as err:
                    raise GtfsError(f"Cannot store the schedule: {err}") from err
                finally:
                    await self._run(partial(shutil.rmtree, workspace, ignore_errors=True))
            finally:
                # The listing is known good here, so whatever it no longer names can go.
                await self._run(partial(_prune_kept, kept_files_dir(self._path), wanted))

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
            name = _kept_name(ref)
            kept = None if name is None else kept_files_dir(self._path) / name
            if kept is not None and await self._run(partial(_is_readable_zip, kept)):
                file_path = kept
            else:
                file_path = workspace / f"{number}.zip"
                await async_download(self._session, ref.url, file_path)
            try:
                outcome = await self._run(
                    partial(
                        self._work,
                        work_path,
                        partial(_apply, ref, file_path, selection, self._provider),
                    )
                )
            except GtfsFeedError:
                # A kept file that will not import is fetched afresh next time.
                if file_path == kept:
                    await self._run(partial(kept.unlink, missing_ok=True))
                raise
            if kept is None:
                await self._run(file_path.unlink)
            elif file_path != kept:
                await self._run(partial(_keep, file_path, kept))
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
        # A file gone from the listing stays until listed files, or newer kept ones, cover what
        # is left of its window. Selection reads the newest snapshot of a date, so an older one
        # under such cover is never read again.
        superseded = set()
        for snapshot in snapshots:
            if snapshot.id in listed or snapshot.id in expired:
                continue
            cover = [
                other
                for other in snapshots
                if other.id in listed or (other.id > snapshot.id and other.id not in expired)
            ]
            start = max(snapshot.valid_from, _local(now, details[snapshot.id][0]).date())
            # With its window over, a kept snapshot is serving night trips, so nothing replaces it.
            if start <= snapshot.valid_to and not missing_dates(cover, start, snapshot.valid_to):
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

    async def _async_trips(self) -> dict[str, catalogue.TripInfo]:
        """Return stored trips by ID, read once per sync."""
        if self._trips is None:
            self._trips = await self._read(catalogue.trip_lines)
        return self._trips

    async def async_trip_delays(self, realtime: RealtimeData) -> list[TripDelay]:
        """Return the live state of each trip in a message, for stored lines only."""
        trips = await self._async_trips()
        return [
            TripDelay(
                TripId(update.trip_id),
                update.start_date,
                trip.line,
                trip.headsign,
                _trip_delay(update),
                update.cancelled,
            )
            for update in realtime.trips.values()
            if (trip := trips.get(update.trip_id)) is not None
        ]

    async def async_fetch_vehicles(self) -> list[Vehicle]:
        """Fetch vehicles of the stored lines. Those the provider cannot identify are left out."""
        url = self._provider.realtime_endpoints().vehicle_positions
        if url is None:
            return []
        body = await self._async_fetch_message(url)
        positions = await self._run(partial(parse_vehicle_positions, body))
        trips = await self._async_trips()
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
                        self._provider.vehicle_label(position.vehicle_id, position.label),
                        trip.shape,
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

    async def async_line_shapes(
        self, key: LineKey, service_date: date | None = None
    ) -> list[Shape]:
        """Return the distinct paths of a stored line. Empty when the source has none.

        With a service date, only the snapshot serving it counts, so other days' variants
        are left out.
        """
        if service_date is None:
            return await self._read(lambda conn: catalogue.line_shapes(conn, key))
        snapshot = await self.async_snapshot_for(service_date)
        if snapshot is None:
            return []
        return await self._read(lambda conn: catalogue.line_shapes(conn, key, snapshot.id))

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
