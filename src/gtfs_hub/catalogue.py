"""Read queries on the stored schedule. Everything here blocks."""

import sqlite3
from collections.abc import Collection
from datetime import date

from .models import Line, LineKey, ResolvedId, Snapshot, Stop
from .text import fold

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
        "SELECT id, version, sha256, valid_from, valid_to FROM snapshots ORDER BY id"
    )
    return [
        Snapshot(id_, version, sha, date.fromisoformat(start), date.fromisoformat(end))
        for id_, version, sha, start, end in rows
    ]


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
    conn: sqlite3.Connection, query: str, limit: int, only: Collection[str] | None = None
) -> list[Stop]:
    """Find stops by name or code, one result per key, optionally among the given keys."""
    rows = conn.execute(
        f"SELECT {_STOP_COLUMNS} FROM stops s WHERE s.search_text LIKE ? ESCAPE '\\'"
        " ORDER BY s.snapshot DESC",
        (_pattern(query),),
    )
    found: dict[str, Stop] = {}
    for row in rows:
        if only is None or row[0] in only:
            found.setdefault(row[0], Stop(*row))
    return sorted(found.values(), key=lambda stop: (fold(stop.name), stop.key))[:limit]


def resolve_line(conn: sqlite3.Connection, key: LineKey) -> list[ResolvedId]:
    """Return the route IDs of a line per snapshot. Empty when it is absent."""
    rows = conn.execute(
        "SELECT snapshot, route_id FROM lines WHERE route_type = ? AND short_name = ?"
        " ORDER BY snapshot, route_id",
        (key.route_type, key.short_name),
    )
    return [ResolvedId(snapshot, route_id) for snapshot, route_id in rows]


def resolve_stop(conn: sqlite3.Connection, key: str) -> list[ResolvedId]:
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
    found: dict[str, Stop] = {}
    for row in rows:
        found.setdefault(row[0], Stop(*row[:6]))
    return sorted(found.values(), key=lambda stop: (fold(stop.name), stop.key))
