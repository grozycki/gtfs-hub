"""Provider-agnostic GTFS schedule library."""

from .discovery import (
    async_create_provider,
    async_list_provider_keys,
    create_provider,
    list_provider_keys,
)
from .download import raise_for_status
from .errors import (
    GtfsAuthError,
    GtfsConnectionError,
    GtfsError,
    GtfsFeedError,
    GtfsNoDataError,
    GtfsRateLimitError,
)
from .generic import GenericProvider
from .hub import GtfsHub
from .models import (
    Capabilities,
    Departure,
    DepartureStatus,
    Line,
    LineKey,
    RealtimeEndpoints,
    ResolvedId,
    Snapshot,
    StaticFeedRef,
    Stop,
    StopChoice,
    StopKey,
    StopKeyField,
    SyncResult,
    TripId,
    Vehicle,
    VehicleClass,
    VehicleKey,
)
from .provider import BaseProvider, Provider, TableReader
from .realtime import RealtimeData

__all__ = [
    "BaseProvider",
    "Capabilities",
    "Departure",
    "DepartureStatus",
    "GenericProvider",
    "GtfsAuthError",
    "GtfsConnectionError",
    "GtfsError",
    "GtfsFeedError",
    "GtfsHub",
    "GtfsNoDataError",
    "GtfsRateLimitError",
    "Line",
    "LineKey",
    "Provider",
    "RealtimeData",
    "RealtimeEndpoints",
    "ResolvedId",
    "Snapshot",
    "StaticFeedRef",
    "Stop",
    "StopChoice",
    "StopKey",
    "StopKeyField",
    "SyncResult",
    "TableReader",
    "TripId",
    "Vehicle",
    "VehicleClass",
    "VehicleKey",
    "async_create_provider",
    "async_list_provider_keys",
    "create_provider",
    "list_provider_keys",
    "raise_for_status",
]
