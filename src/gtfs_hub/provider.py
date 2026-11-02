"""The contract between core and providers."""

from collections.abc import Callable, Iterable, Mapping
from typing import Protocol, runtime_checkable

from aiohttp import ClientSession

from .models import Capabilities, RealtimeEndpoints, StaticFeedRef, StopKeyField, VehicleClass

# Reads a table by its name without .txt. An absent table gives no rows.
type TableReader = Callable[[str], Iterable[Mapping[str, str]]]


@runtime_checkable
class Provider(Protocol):
    """A source of GTFS data."""

    @property
    def key(self) -> str:
        """Return the stable key that consumers store."""
        ...

    @property
    def name(self) -> str:
        """Return the display name."""
        ...

    @property
    def attribution(self) -> str | None:
        """Return the licence text consumers must show."""
        ...

    @property
    def stop_key(self) -> StopKeyField:
        """Return the field that identifies a stop across snapshots."""
        ...

    @property
    def capabilities(self) -> Capabilities:
        """Return what this source really supplies."""
        ...

    async def async_list_static_feeds(self, session: ClientSession) -> list[StaticFeedRef]:
        """Return the current schedule files."""
        ...

    def realtime_endpoints(self) -> RealtimeEndpoints:
        """Return where live data is."""
        ...

    def normalize_static(self, table: str, row: dict[str, str]) -> dict[str, str]:
        """Return a schedule row, adjusted for quirks of the source."""
        ...

    def vehicle_key(self, vehicle_id: str, label: str, trip_id: str) -> str | None:
        """Return an identity that survives a change of trip, or None."""
        ...

    def vehicle_label(self, vehicle_id: str, label: str) -> str | None:
        """Return the fleet number riders see, or None."""
        ...

    def trip_vehicle_classes(self, read: TableReader) -> Mapping[str, VehicleClass]:
        """Return the planned vehicle by trip, from any table of a file."""
        ...


class BaseProvider:
    """Defaults for a provider, which overrides only what it needs."""

    attribution: str | None = None
    stop_key: StopKeyField = "stop_id"
    capabilities: Capabilities = Capabilities()

    def realtime_endpoints(self) -> RealtimeEndpoints:
        """Return no endpoints."""
        return RealtimeEndpoints()

    def normalize_static(self, table: str, row: dict[str, str]) -> dict[str, str]:
        """Return the row unchanged."""
        return row

    def vehicle_key(self, vehicle_id: str, label: str, trip_id: str) -> str | None:
        """Return no identity: a raw vehicle ID proves nothing."""
        return None

    def vehicle_label(self, vehicle_id: str, label: str) -> str | None:
        """Return the feed's label, which GTFS Realtime defines as visible to riders."""
        return label or None

    def trip_vehicle_classes(self, read: TableReader) -> Mapping[str, VehicleClass]:
        """Return no classes: GTFS has no such table."""
        return {}
