"""Read queries on the stored schedule. Everything here blocks."""

import re
import sqlite3
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date

from .models import (
    Line,
    LineKey,
    ResolvedId,
    Shape,
    ShapeId,
    Snapshot,
    Stop,
    StopChoice,
    StopKey,
    VehicleClass,
)
from .text import fold


@dataclass(frozen=True, slots=True)
class TripInfo:
    """What a vehicle position needs of its trip."""

    line: LineKey
    headsign: str | None
    planned_class: VehicleClass | None
    shape: Shape | None = None


_LINE_COLUMNS = """
    l.route_type, l.short_name, l.long_name, a.name, l.color, l.text_color, l.url
    FROM lines l
    LEFT JOIN agencies a ON a.snapshot = l.snapshot AND a.agency_id = l.agency_id
"""
_STOP_COLUMNS = "s.stop_key, s.name, s.code, s.latitude, s.longitude, s.url"


def _pattern(query: str) -> str:
    escaped = fold(query.strip()).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _line(row: tuple[int, str, str | None, str | None, str | None, str | None, str | None]) -> Line:
    return Line(LineKey(row[0], row[1]), row[2], row[3], row[4], row[5], row[6])


def read_snapshots(conn: sqlite3.Connection) -> list[Snapshot]:
    """Return all stored snapshots, oldest first."""
    rows = conn.execute(
        "SELECT id, version, fingerprint, valid_from, valid_to FROM snapshots ORDER BY id"
    )
    return [
        Snapshot(id_, version, sha, date.fromisoformat(start), date.fromisoformat(end))
        for id_, version, sha, start, end in rows
    ]


def natural_key(text: str) -> list[tuple[int, int, str]]:
    """Return a sort key that puts 2 before 10."""
    return [
        (0, int(part), "") if part.isdecimal() else (1, 0, part)
        for part in re.split(r"(\d+)", fold(text))
        if part
    ]


def list_lines(conn: sqlite3.Connection) -> list[Line]:
    """Return every line once, in natural order of short names."""
    rows = conn.execute(f"SELECT {_LINE_COLUMNS} ORDER BY l.snapshot DESC")
    found: dict[LineKey, Line] = {}
    for row in rows:
        line = _line(row)
        found.setdefault(line.key, line)
    return sorted(found.values(), key=lambda line: natural_key(line.key.short_name))


def search_lines(conn: sqlite3.Connection, query: str, limit: int) -> list[Line]:
    """Find lines by short or long name, one result per key, exact numbers first."""
    rows = conn.execute(
        f"SELECT {_LINE_COLUMNS} WHERE l.search_text LIKE ? ESCAPE '\\' ORDER BY l.snapshot DESC",
        (_pattern(query),),
    )
    found: dict[LineKey, Line] = {}
    for row in rows:
        line = _line(row)
        found.setdefault(line.key, line)
    folded = fold(query.strip())
    ordered = sorted(
        found.values(), key=lambda line: (fold(line.key.short_name) != folded, line.key)
    )
    return ordered[:limit]


def search_stops(
    conn: sqlite3.Connection, query: str, limit: int, only: Collection[StopKey] | None = None
) -> list[Stop]:
    """Find stops by name or code, one result per key, optionally among the given keys."""
    rows = conn.execute(
        f"SELECT {_STOP_COLUMNS} FROM stops s WHERE s.search_text LIKE ? ESCAPE '\\'"
        " ORDER BY s.snapshot DESC",
        (_pattern(query),),
    )
    found: dict[StopKey, Stop] = {}
    for row in rows:
        if only is None or row[0] in only:
            found.setdefault(row[0], Stop(StopKey(row[0]), *row[1:]))
    return sorted(found.values(), key=lambda stop: (fold(stop.name), stop.key))[:limit]


def resolve_line(conn: sqlite3.Connection, key: LineKey) -> list[ResolvedId]:
    """Return the route IDs of a line per snapshot. Empty when it is absent."""
    rows = conn.execute(
        "SELECT snapshot, route_id FROM lines WHERE route_type = ? AND short_name = ?"
        " ORDER BY snapshot, route_id",
        (key.route_type, key.short_name),
    )
    return [ResolvedId(snapshot, route_id) for snapshot, route_id in rows]


def resolve_stop(conn: sqlite3.Connection, key: StopKey) -> list[ResolvedId]:
    """Return the stop IDs of a stop per snapshot. Empty when it is absent."""
    rows = conn.execute(
        "SELECT snapshot, stop_id FROM stops WHERE stop_key = ? ORDER BY snapshot, stop_id",
        (key,),
    )
    return [ResolvedId(snapshot, stop_id) for snapshot, stop_id in rows]


def stops_of_line(conn: sqlite3.Connection, key: LineKey) -> list[Stop]:
    """Return the stops a selected line serves in any snapshot and direction."""
    rows = conn.execute(
        f"""
        SELECT DISTINCT {_STOP_COLUMNS}, s.snapshot
        FROM lines l
        JOIN trips t ON t.snapshot = l.snapshot AND t.route_id = l.route_id
        JOIN stop_times st ON st.snapshot = t.snapshot AND st.trip_id = t.trip_id
        JOIN stops s ON s.snapshot = st.snapshot AND s.stop_id = st.stop_id
        WHERE l.route_type = ? AND l.short_name = ?
            AND NOT (st.pickup_type = 1 AND st.drop_off_type = 1)
        ORDER BY s.snapshot DESC
        """,
        (key.route_type, key.short_name),
    )
    found: dict[StopKey, Stop] = {}
    for row in rows:
        found.setdefault(row[0], Stop(StopKey(row[0]), *row[1:6]))
    return sorted(found.values(), key=lambda stop: (fold(stop.name), stop.key))


def stop_choices(conn: sqlite3.Connection, keys: Collection[LineKey]) -> list[StopChoice]:
    """Return the passenger stops of stored lines, with their lines and headsigns."""
    stops: dict[StopKey, Stop] = {}
    lines: dict[StopKey, set[str]] = {}
    headsigns: dict[StopKey, set[str]] = {}
    for key in keys:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {_STOP_COLUMNS}, s.snapshot,
                st.pickup_type != 1 AND st.stop_sequence < (
                    SELECT max(last.stop_sequence) FROM stop_times last
                    WHERE last.snapshot = st.snapshot AND last.trip_id = st.trip_id
                ),
                coalesce(st.headsign, t.headsign)
            FROM lines l
            JOIN trips t ON t.snapshot = l.snapshot AND t.route_id = l.route_id
            JOIN stop_times st ON st.snapshot = t.snapshot AND st.trip_id = t.trip_id
            JOIN stops s ON s.snapshot = st.snapshot AND s.stop_id = st.stop_id
            WHERE l.route_type = ? AND l.short_name = ?
                AND NOT (st.pickup_type = 1 AND st.drop_off_type = 1)
            ORDER BY s.snapshot DESC
            """,
            (key.route_type, key.short_name),
        )
        for row in rows:
            stop_key = StopKey(row[0])
            stops.setdefault(stop_key, Stop(stop_key, *row[1:6]))
            lines.setdefault(stop_key, set()).add(key.short_name)
            if row[7] and row[8]:
                headsigns.setdefault(stop_key, set()).add(row[8])
    return sorted(
        (
            StopChoice(
                stop,
                tuple(sorted(lines[key], key=natural_key)),
                tuple(sorted(headsigns.get(key, ()))),
            )
            for key, stop in stops.items()
        ),
        key=lambda choice: (fold(choice.stop.name), choice.headsigns, choice.stop.key),
    )


def _read_shapes(conn: sqlite3.Connection, key: LineKey | None) -> dict[tuple[int, str], Shape]:
    """Read stored shapes by snapshot and ID, of one line or of all.

    Each takes the line, headsign and direction most of its trips state.
    """
    line_filter = "" if key is None else "AND l.route_type = ? AND l.short_name = ?"
    params = () if key is None else (key.route_type, key.short_name)
    counts: dict[tuple[int, str], dict[tuple[int, str, str | None, int | None], int]] = {}
    for snapshot, shape_id, route_type, short_name, headsign, direction, trips in conn.execute(
        f"""
        SELECT t.snapshot, t.shape_id, l.route_type, l.short_name, t.headsign, t.direction_id,
            count(*)
        FROM trips t
        JOIN lines l ON l.snapshot = t.snapshot AND l.route_id = t.route_id
        WHERE t.shape_id IS NOT NULL {line_filter}
        GROUP BY 1, 2, 3, 4, 5, 6
        """,
        params,
    ):
        counts.setdefault((snapshot, shape_id), {})[
            (route_type, short_name, headsign, direction)
        ] = trips
    points: dict[tuple[int, str], list[tuple[float, float]]] = {}
    for snapshot, shape_id, latitude, longitude in conn.execute(
        """
        SELECT snapshot, shape_id, latitude, longitude FROM shape_points
        ORDER BY snapshot, shape_id, sequence
        """
    ):
        if (snapshot, shape_id) in counts:
            points.setdefault((snapshot, shape_id), []).append((latitude, longitude))
    shapes = {}
    for ref, path in points.items():
        options = counts[ref]
        route_type, short_name, headsign, direction = max(options, key=lambda o: options[o])
        shapes[ref] = Shape(
            ShapeId(ref[1]), LineKey(route_type, short_name), headsign, direction, tuple(path)
        )
    return shapes


def line_shapes(conn: sqlite3.Connection, key: LineKey) -> list[Shape]:
    """Return each distinct path of a stored line, the newest snapshot winning an ID."""
    by_id: dict[str, Shape] = {}
    for (_, shape_id), shape in sorted(_read_shapes(conn, key).items(), reverse=True):
        by_id.setdefault(shape_id, shape)
    distinct: dict[tuple[tuple[float, float], ...], Shape] = {}
    for shape in by_id.values():
        distinct.setdefault(shape.points, shape)
    return sorted(
        distinct.values(),
        key=lambda shape: (shape.direction_id is None, shape.direction_id, shape.headsign or ""),
    )


def trip_lines(conn: sqlite3.Connection) -> dict[str, TripInfo]:
    """Return what vehicles need of every stored trip, the newest snapshot winning."""
    shapes = _read_shapes(conn, None)
    rows = conn.execute(
        """
        SELECT t.trip_id, l.route_type, l.short_name, t.headsign, t.vehicle_name,
            t.vehicle_length, t.vehicle_units, t.vehicle_low_floor, t.vehicle_description,
            t.snapshot, t.shape_id
        FROM trips t
        JOIN lines l ON l.snapshot = t.snapshot AND l.route_id = t.route_id
        ORDER BY t.snapshot
        """
    )
    return {
        row[0]: TripInfo(
            LineKey(row[1], row[2]),
            row[3],
            None
            if row[4] is None
            else VehicleClass(
                row[4], row[5], row[6], None if row[7] is None else bool(row[7]), row[8]
            ),
            shapes.get((row[9], row[10])),
        )
        for row in rows
    }
