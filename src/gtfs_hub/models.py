"""Public data models."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

type StopKeyField = Literal["stop_id", "stop_code"]


@dataclass(frozen=True, slots=True)
class StaticFeedRef:
    """One downloadable schedule file."""

    url: str
    version: str | None = None
    valid_from: date | None = None
    valid_to: date | None = None


@dataclass(frozen=True, slots=True)
class RealtimeEndpoints:
    """Where live data is. Any of them may be absent."""

    trip_updates: str | None = None
    vehicle_positions: str | None = None
    alerts: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict, hash=False)


@dataclass(frozen=True, slots=True)
class Capabilities:
    """What a source really supplies. Undeclared means absent."""

    vehicle_positions: bool = False
    stable_vehicle_id: bool = False
    bearing: bool = False
    speed: bool = False
    occupancy: bool = False
    alerts: bool = False
    shapes: bool = False
    trip_delays: bool = False


@dataclass(frozen=True, slots=True, order=True)
class LineKey:
    """Identity of a line that survives republishing."""

    route_type: int
    short_name: str


@dataclass(frozen=True, slots=True)
class Line:
    """A line from the catalogue."""

    key: LineKey
    long_name: str | None
    agency: str | None
    color: str | None
    text_color: str | None


@dataclass(frozen=True, slots=True)
class Stop:
    """A stop from the catalogue."""

    key: str
    name: str
    code: str | None
    latitude: float | None
    longitude: float | None


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One imported schedule file."""

    id: int
    version: str | None
    sha256: str
    valid_from: date
    valid_to: date


@dataclass(frozen=True, slots=True)
class ResolvedId:
    """A source ID valid within one snapshot."""

    snapshot_id: int
    id: str


@dataclass(frozen=True, slots=True)
class SyncResult:
    """What one synchronisation did, counted in files."""

    imported: int = 0
    relabelled: int = 0
    skipped: int = 0
    extended: int = 0
    removed: int = 0
