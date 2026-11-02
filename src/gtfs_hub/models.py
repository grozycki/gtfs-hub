"""Public data models."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Literal, NewType

type StopKeyField = Literal["stop_id", "stop_code"]

StopKey = NewType("StopKey", str)
TripId = NewType("TripId", str)
VehicleKey = NewType("VehicleKey", str)


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
    url: str | None = None


@dataclass(frozen=True, slots=True)
class Stop:
    """A stop from the catalogue."""

    key: StopKey
    name: str
    code: str | None
    latitude: float | None
    longitude: float | None
    url: str | None = None


@dataclass(frozen=True, slots=True)
class StopChoice:
    """A stop offered for selection, with what tells it from others of the same name."""

    stop: Stop
    lines: tuple[str, ...]
    headsigns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One imported schedule file."""

    id: int
    version: str | None
    fingerprint: str
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


@dataclass(frozen=True, slots=True)
class VehicleClass:
    """The vehicle planned for a trip. The one that comes may differ."""

    name: str
    length: int | None = None
    units: int = 1
    low_floor: bool | None = None


@dataclass(frozen=True, slots=True)
class Vehicle:
    """A vehicle on a selected line. Optional values stay None when not sent."""

    key: VehicleKey
    line: LineKey
    trip_id: TripId
    headsign: str | None
    latitude: float
    longitude: float
    measured: datetime | None
    planned_class: VehicleClass | None = None
    # The number riders see on the vehicle, which the provider may derive.
    label: str | None = None


class DepartureStatus(StrEnum):
    """How far a departure can be trusted."""

    SCHEDULED = "scheduled"
    PREDICTED = "predicted"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class Departure:
    """One departure from a stop. Times are in UTC."""

    stop_key: StopKey
    line: LineKey
    headsign: str | None
    scheduled: datetime
    predicted: datetime | None
    delay: int | None
    status: DepartureStatus
    trip_id: TripId
    service_date: date

    @property
    def expected(self) -> datetime:
        """Return the best known time."""
        return self.predicted or self.scheduled
