"""The SQLite cache: schema and connections."""

import sqlite3
from pathlib import Path

from .errors import GtfsNoDataError

SCHEMA_VERSION = 5

_SCHEMA = """
CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT,
    fingerprint TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL,
    timezone TEXT,
    max_seconds INTEGER
);
CREATE TABLE imported_lines (
    snapshot INTEGER NOT NULL,
    route_type INTEGER NOT NULL,
    short_name TEXT NOT NULL,
    PRIMARY KEY (snapshot, route_type, short_name)
) WITHOUT ROWID;
CREATE TABLE agencies (
    snapshot INTEGER NOT NULL,
    agency_id TEXT NOT NULL,
    name TEXT,
    PRIMARY KEY (snapshot, agency_id)
) WITHOUT ROWID;
CREATE TABLE lines (
    snapshot INTEGER NOT NULL,
    route_id TEXT NOT NULL,
    route_type INTEGER NOT NULL,
    short_name TEXT NOT NULL,
    long_name TEXT,
    agency_id TEXT,
    color TEXT,
    text_color TEXT,
    search_text TEXT NOT NULL,
    url TEXT,
    PRIMARY KEY (snapshot, route_id)
) WITHOUT ROWID;
CREATE INDEX lines_key ON lines (route_type, short_name);
CREATE TABLE stops (
    snapshot INTEGER NOT NULL,
    stop_id TEXT NOT NULL,
    stop_key TEXT NOT NULL,
    code TEXT,
    name TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    search_text TEXT NOT NULL,
    url TEXT,
    PRIMARY KEY (snapshot, stop_id)
) WITHOUT ROWID;
CREATE INDEX stops_key ON stops (stop_key);
CREATE TABLE services (
    snapshot INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    weekdays TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    PRIMARY KEY (snapshot, service_id)
) WITHOUT ROWID;
CREATE TABLE service_exceptions (
    snapshot INTEGER NOT NULL,
    day TEXT NOT NULL,
    service_id TEXT NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (snapshot, day, service_id)
) WITHOUT ROWID;
CREATE TABLE trips (
    snapshot INTEGER NOT NULL,
    trip_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    direction_id INTEGER,
    service_id TEXT NOT NULL,
    headsign TEXT,
    vehicle_name TEXT,
    vehicle_length INTEGER,
    vehicle_units INTEGER,
    vehicle_low_floor INTEGER,
    vehicle_description TEXT,
    PRIMARY KEY (snapshot, trip_id)
) WITHOUT ROWID;
CREATE INDEX trips_route ON trips (snapshot, route_id);
CREATE TABLE stop_times (
    snapshot INTEGER NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,
    arrival_seconds INTEGER,
    departure_seconds INTEGER,
    pickup_type INTEGER NOT NULL,
    drop_off_type INTEGER NOT NULL,
    headsign TEXT
);
CREATE INDEX stop_times_trip ON stop_times (snapshot, trip_id);
CREATE INDEX stop_times_stop ON stop_times (snapshot, stop_id, departure_seconds);
"""


def stored_version(path: Path) -> int | None:
    """Return the schema version of a database, or None when it is unreadable."""
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            row = conn.execute("PRAGMA user_version").fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return None
    return int(row[0])


def open_work(path: Path) -> sqlite3.Connection:
    """Open a disposable working copy, creating its schema when it is new."""
    new = not path.exists()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    if new:
        conn.executescript(_SCHEMA)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.commit()
    return conn


def open_read(path: Path) -> sqlite3.Connection:
    """Open the live database for one read job."""
    try:
        conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    except sqlite3.DatabaseError as err:
        raise GtfsNoDataError(f"No usable schedule database at {path}") from err
    if version != SCHEMA_VERSION:
        conn.close()
        raise GtfsNoDataError(f"No usable schedule database at {path}")
    return conn
