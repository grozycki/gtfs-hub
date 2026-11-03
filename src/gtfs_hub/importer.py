"""Import schedule files into a working database. Everything here blocks."""

import csv
import hashlib
import io
import logging
import sqlite3
import zipfile
from collections.abc import Collection, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from itertools import islice
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import GtfsFeedError
from .models import LineKey, StaticFeedRef
from .provider import Provider
from .text import fold

type SqlRow = tuple[str | int | float | None, ...]

_BATCH_SIZE = 5_000
_WEEKDAY_COLUMNS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FileOutcome:
    """What happened to one downloaded file."""

    snapshot_id: int
    action: str  # imported, relabelled or extended


def _rows(
    archive: zipfile.ZipFile, table: str, required: tuple[str, ...], provider: Provider
) -> Iterator[dict[str, str]]:
    """Stream a table from an open archive, one normalised row at a time."""
    member = f"{table}.txt"
    if member not in archive.namelist():
        raise GtfsFeedError(f"Missing {member}")
    with (
        archive.open(member) as raw,
        io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text,
    ):
        reader = csv.DictReader(text, restval="")
        columns = [name.strip() for name in reader.fieldnames or ()]
        reader.fieldnames = columns
        absent = set(required) - set(columns)
        if absent:
            raise GtfsFeedError(f"{member} lacks {', '.join(sorted(absent))}")
        for row in reader:
            yield provider.normalize_static(table, row)


def _insert_batches(conn: sqlite3.Connection, sql: str, rows: Iterable[SqlRow]) -> None:
    iterator = iter(rows)
    while batch := list(islice(iterator, _BATCH_SIZE)):
        conn.executemany(sql, batch)


def _empty_to_none(value: str | None) -> str | None:
    return (value or "").strip() or None


def _float_or_none(value: str | None) -> float | None:
    text = _empty_to_none(value)
    return None if text is None else float(text)


def _parse_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y%m%d").date()


def _parse_time(value: str) -> int | None:
    """Return seconds from the start of the service day. Hours may pass 24."""
    if not value:
        return None
    hours, minutes, seconds = value.strip().split(":")
    return int(hours) * 3_600 + int(minutes) * 60 + int(seconds)


def _read_window(archive: zipfile.ZipFile, provider: Provider) -> tuple[date, date]:
    """Read a validity window from feed_info, or else from the calendar."""
    names = archive.namelist()
    if "feed_info.txt" in names:
        for row in _rows(archive, "feed_info", (), provider):
            start, end = row.get("feed_start_date"), row.get("feed_end_date")
            if start and end:
                return _parse_date(start), _parse_date(end)
    starts: list[date] = []
    ends: list[date] = []
    if "calendar.txt" in names:
        for row in _rows(archive, "calendar", ("start_date", "end_date"), provider):
            starts.append(_parse_date(row["start_date"]))
            ends.append(_parse_date(row["end_date"]))
    if "calendar_dates.txt" in names:
        for row in _rows(archive, "calendar_dates", ("date",), provider):
            day = _parse_date(row["date"])
            starts.append(day)
            ends.append(day)
    if not starts:
        raise GtfsFeedError("The file states no validity window")
    return min(starts), max(ends)


def _fingerprint(archive: zipfile.ZipFile, valid_from: date, valid_to: date) -> str:
    """Identify the content of a file, read from the ZIP directory with nothing unpacked.

    The whole file will not do: publishers rebuild it daily with a new date in feed_info.txt.
    """
    members = sorted(
        (info.filename, info.CRC, info.file_size)
        for info in archive.infolist()
        if info.filename != "feed_info.txt"
    )
    return hashlib.sha256(repr((valid_from, valid_to, members)).encode()).hexdigest()


def _read_timezone(archive: zipfile.ZipFile, provider: Provider) -> str | None:
    if "agency.txt" not in archive.namelist():
        return None
    for row in _rows(archive, "agency", (), provider):
        name = _empty_to_none(row.get("agency_timezone"))
        if name is None:
            continue
        try:
            ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError):
            return None
        return name
    return None


def _line_key(row: dict[str, str]) -> LineKey:
    name = (row.get("route_short_name") or "").strip() or (row.get("route_long_name") or "").strip()
    return LineKey(int(row["route_type"]), name)


def _import_catalogue(
    conn: sqlite3.Connection, archive: zipfile.ZipFile, snapshot: int, provider: Provider
) -> None:
    agencies: list[SqlRow] = []
    if "agency.txt" in archive.namelist():
        agencies = [
            (snapshot, row.get("agency_id", ""), _empty_to_none(row.get("agency_name")))
            for row in _rows(archive, "agency", (), provider)
        ]
        conn.executemany("INSERT OR REPLACE INTO agencies VALUES (?, ?, ?)", agencies)
    # With a single agency, routes may leave agency_id out.
    only_agency = agencies[0][1] if len(agencies) == 1 else ""

    def line(row: dict[str, str]) -> SqlRow:
        key = _line_key(row)
        long_name = _empty_to_none(row.get("route_long_name"))
        return (
            snapshot,
            row["route_id"],
            key.route_type,
            key.short_name,
            long_name,
            row.get("agency_id") or only_agency,
            _empty_to_none(row.get("route_color")),
            _empty_to_none(row.get("route_text_color")),
            fold(f"{key.short_name} {long_name or ''}"),
            _empty_to_none(row.get("route_url")),
        )

    _insert_batches(
        conn,
        "INSERT OR REPLACE INTO lines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (line(row) for row in _rows(archive, "routes", ("route_id", "route_type"), provider)),
    )

    def stop(row: dict[str, str]) -> SqlRow:
        code = _empty_to_none(row.get("stop_code"))
        name = (row.get("stop_name") or "").strip()
        key = _empty_to_none(row.get(provider.stop_key)) or row["stop_id"]
        return (
            snapshot,
            row["stop_id"],
            key,
            code,
            name,
            _float_or_none(row.get("stop_lat")),
            _float_or_none(row.get("stop_lon")),
            fold(f"{name} {code or ''}"),
            _empty_to_none(row.get("stop_url")),
        )

    _insert_batches(
        conn,
        "INSERT OR REPLACE INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            stop(row)
            for row in _rows(archive, "stops", ("stop_id",), provider)
            if (row.get("location_type") or "0").strip() == "0"
        ),
    )


def _import_services(
    conn: sqlite3.Connection, archive: zipfile.ZipFile, snapshot: int, provider: Provider
) -> None:
    names = archive.namelist()
    if "calendar.txt" in names:
        required = ("service_id", *_WEEKDAY_COLUMNS, "start_date", "end_date")
        _insert_batches(
            conn,
            "INSERT OR REPLACE INTO services VALUES (?, ?, ?, ?, ?)",
            (
                (
                    snapshot,
                    row["service_id"],
                    "".join(row[day].strip() for day in _WEEKDAY_COLUMNS),
                    _parse_date(row["start_date"]).isoformat(),
                    _parse_date(row["end_date"]).isoformat(),
                )
                for row in _rows(archive, "calendar", required, provider)
            ),
        )
    if "calendar_dates.txt" in names:
        _insert_batches(
            conn,
            "INSERT OR REPLACE INTO service_exceptions VALUES (?, ?, ?, ?)",
            (
                (
                    snapshot,
                    _parse_date(row["date"]).isoformat(),
                    row["service_id"],
                    int(row["exception_type"]),
                )
                for row in _rows(
                    archive, "calendar_dates", ("service_id", "date", "exception_type"), provider
                )
            ),
        )


def _frequency_trips(archive: zipfile.ZipFile, provider: Provider) -> set[str]:
    """Return trips that stand for many: their stop times are not a timetable."""
    if "frequencies.txt" not in archive.namelist():
        return set()
    trips = {row["trip_id"] for row in _rows(archive, "frequencies", ("trip_id",), provider)}
    if trips:
        _LOGGER.warning("Leaving out %d headway trips: frequencies.txt is unsupported", len(trips))
    return trips


def _import_timetables(
    conn: sqlite3.Connection,
    archive: zipfile.ZipFile,
    snapshot: int,
    lines: Collection[LineKey],
    provider: Provider,
) -> None:
    """Store the trips and stop times of the given lines."""
    conn.executemany(
        "INSERT OR IGNORE INTO imported_lines VALUES (?, ?, ?)",
        [(snapshot, line.route_type, line.short_name) for line in lines],
    )
    routes = {
        route_id
        for route_id, route_type, short_name in conn.execute(
            "SELECT route_id, route_type, short_name FROM lines WHERE snapshot = ?", (snapshot,)
        )
        if LineKey(route_type, short_name) in lines
    }
    if not routes:
        return
    trips: set[str] = set()
    headway_trips = _frequency_trips(archive, provider)

    def read_table(table: str) -> Iterator[dict[str, str]]:
        if f"{table}.txt" in archive.namelist():
            yield from _rows(archive, table, (), provider)

    classes = provider.trip_vehicle_classes(read_table)

    def trip_rows() -> Iterator[SqlRow]:
        for row in _rows(archive, "trips", ("route_id", "trip_id", "service_id"), provider):
            if row["route_id"] in routes and row["trip_id"] not in headway_trips:
                trips.add(row["trip_id"])
                planned = classes.get(row["trip_id"])
                yield (
                    snapshot,
                    row["trip_id"],
                    row["route_id"],
                    _empty_to_none(row.get("direction_id")),
                    row["service_id"],
                    _empty_to_none(row.get("trip_headsign")),
                    planned.name if planned else None,
                    planned.length if planned else None,
                    planned.units if planned else None,
                    planned.low_floor if planned else None,
                    planned.description if planned else None,
                    _empty_to_none(row.get("shape_id")),
                )

    _insert_batches(
        conn,
        "INSERT OR REPLACE INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        trip_rows(),
    )
    _import_shapes(conn, archive, snapshot, provider)
    latest = 0

    def stop_time_rows() -> Iterator[SqlRow]:
        nonlocal latest
        required = ("trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time")
        for row in _rows(archive, "stop_times", required, provider):
            arrival = _parse_time(row["arrival_time"])
            departure = _parse_time(row["departure_time"])
            departure = arrival if departure is None else departure
            latest = max(latest, arrival or 0, departure or 0)
            if row["trip_id"] in trips:
                yield (
                    snapshot,
                    row["trip_id"],
                    int(row["stop_sequence"]),
                    row["stop_id"],
                    arrival,
                    departure,
                    int(row.get("pickup_type") or 0),
                    int(row.get("drop_off_type") or 0),
                    _empty_to_none(row.get("stop_headsign")),
                )

    _insert_batches(
        conn, "INSERT INTO stop_times VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stop_time_rows()
    )
    conn.execute(
        "UPDATE snapshots SET max_seconds = max(coalesce(max_seconds, 0), ?) WHERE id = ?",
        (latest, snapshot),
    )


def _import_shapes(
    conn: sqlite3.Connection, archive: zipfile.ZipFile, snapshot: int, provider: Provider
) -> None:
    """Store the shapes stored trips use and the snapshot lacks, in one pass over the file."""
    if "shapes.txt" not in archive.namelist():
        return
    wanted = {
        shape_id
        for (shape_id,) in conn.execute(
            "SELECT DISTINCT shape_id FROM trips t WHERE snapshot = ? AND shape_id IS NOT NULL"
            " AND NOT EXISTS (SELECT 1 FROM shape_points p"
            " WHERE p.snapshot = t.snapshot AND p.shape_id = t.shape_id)",
            (snapshot,),
        )
    }
    if not wanted:
        return
    required = ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")
    _insert_batches(
        conn,
        "INSERT OR REPLACE INTO shape_points VALUES (?, ?, ?, ?, ?)",
        (
            (
                snapshot,
                row["shape_id"],
                int(row["shape_pt_sequence"]),
                float(row["shape_pt_lat"]),
                float(row["shape_pt_lon"]),
            )
            for row in _rows(archive, "shapes", required, provider)
            if row["shape_id"] in wanted
        ),
    )


def _imported_lines(conn: sqlite3.Connection, snapshot: int) -> set[LineKey]:
    rows = conn.execute(
        "SELECT route_type, short_name FROM imported_lines WHERE snapshot = ?", (snapshot,)
    )
    return {LineKey(route_type, short_name) for route_type, short_name in rows}


def needs_download(
    conn: sqlite3.Connection, ref: StaticFeedRef, selection: Collection[LineKey]
) -> tuple[int | None, bool]:
    """Return the snapshot matching a listed version, and whether a download is needed."""
    if ref.version is None:
        return None, True
    row = conn.execute("SELECT id FROM snapshots WHERE version = ?", (ref.version,)).fetchone()
    if row is None:
        return None, True
    return row[0], bool(set(selection) - _imported_lines(conn, row[0]))


def apply_file(
    conn: sqlite3.Connection,
    ref: StaticFeedRef,
    path: Path,
    selection: Collection[LineKey],
    provider: Provider,
) -> FileOutcome:
    """Import a downloaded file, doing only what the stored data lacks."""
    try:
        with zipfile.ZipFile(path) as archive:
            return _apply_archive(conn, ref, archive, selection, provider)
    except (
        zipfile.BadZipFile,
        csv.Error,
        UnicodeDecodeError,
        ValueError,
        KeyError,
    ) as err:
        raise GtfsFeedError(f"Unusable file {ref.url}: {err!r}") from err


def _apply_archive(
    conn: sqlite3.Connection,
    ref: StaticFeedRef,
    archive: zipfile.ZipFile,
    selection: Collection[LineKey],
    provider: Provider,
) -> FileOutcome:
    window = None
    if ref.valid_from is None or ref.valid_to is None:
        window = _read_window(archive, provider)
    valid_from = ref.valid_from or (window[0] if window else None)
    valid_to = ref.valid_to or (window[1] if window else None)
    assert valid_from is not None and valid_to is not None
    fingerprint = _fingerprint(archive, valid_from, valid_to)
    row = conn.execute(
        "SELECT id, version FROM snapshots WHERE fingerprint = ? ORDER BY id DESC", (fingerprint,)
    ).fetchone()
    if row is None:
        cursor = conn.execute(
            "INSERT INTO snapshots (version, fingerprint, valid_from, valid_to, timezone)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                ref.version,
                fingerprint,
                valid_from.isoformat(),
                valid_to.isoformat(),
                _read_timezone(archive, provider),
            ),
        )
        snapshot = cursor.lastrowid
        assert snapshot is not None
        _import_catalogue(conn, archive, snapshot, provider)
        _import_services(conn, archive, snapshot, provider)
        action = "imported"
    else:
        snapshot, stored_version = row
        conn.execute("UPDATE snapshots SET version = ? WHERE id = ?", (ref.version, snapshot))
        action = "relabelled" if stored_version != ref.version else "unchanged"
    missing = set(selection) - _imported_lines(conn, snapshot)
    if missing:
        _import_timetables(conn, archive, snapshot, missing, provider)
        if action != "imported":
            action = "extended"
    conn.commit()
    return FileOutcome(snapshot, action)


TABLES_BY_SNAPSHOT = (
    "shape_points",
    "stop_times",
    "trips",
    "stops",
    "lines",
    "agencies",
    "imported_lines",
    "services",
    "service_exceptions",
)


def remove_deselected(conn: sqlite3.Connection, selection: Collection[LineKey]) -> bool:
    """Delete timetables of lines no longer selected. Returns whether any were."""
    conn.execute("CREATE TEMP TABLE selected (route_type INTEGER, short_name TEXT)")
    conn.executemany(
        "INSERT INTO selected VALUES (?, ?)",
        [(line.route_type, line.short_name) for line in selection],
    )
    outdated = """
        SELECT 1 FROM selected s
        WHERE s.route_type = {table}.route_type AND s.short_name = {table}.short_name
    """
    removed = conn.execute(
        f"DELETE FROM imported_lines WHERE NOT EXISTS ({outdated.format(table='imported_lines')})"
    ).rowcount
    conn.execute(
        "CREATE TEMP TABLE stale_trips AS"
        " SELECT t.snapshot, t.trip_id FROM trips t"
        " JOIN lines ON lines.snapshot = t.snapshot AND lines.route_id = t.route_id"
        f" WHERE NOT EXISTS ({outdated.format(table='lines')})"
    )
    conn.execute(
        "DELETE FROM stop_times WHERE (snapshot, trip_id) IN"
        " (SELECT snapshot, trip_id FROM stale_trips)"
    )
    conn.execute(
        "DELETE FROM trips WHERE (snapshot, trip_id) IN (SELECT snapshot, trip_id FROM stale_trips)"
    )
    conn.execute("DROP TABLE stale_trips")
    conn.execute(
        "DELETE FROM shape_points WHERE (snapshot, shape_id) NOT IN"
        " (SELECT snapshot, shape_id FROM trips WHERE shape_id IS NOT NULL)"
    )
    conn.execute("DROP TABLE selected")
    conn.commit()
    return removed > 0


def delete_snapshots(conn: sqlite3.Connection, snapshots: Collection[int]) -> None:
    """Delete snapshots and every row scoped to them."""
    for snapshot in snapshots:
        for table in TABLES_BY_SNAPSHOT:
            conn.execute(f"DELETE FROM {table} WHERE snapshot = ?", (snapshot,))
        conn.execute("DELETE FROM snapshots WHERE id = ?", (snapshot,))
    conn.commit()
