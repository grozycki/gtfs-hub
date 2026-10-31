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
    Line,
    LineKey,
    RealtimeEndpoints,
    ResolvedId,
    Snapshot,
    StaticFeedRef,
    Stop,
    StopKeyField,
    SyncResult,
)
from .provider import BaseProvider, Provider

__all__ = [
    "BaseProvider",
    "Capabilities",
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
    "RealtimeEndpoints",
    "ResolvedId",
    "Snapshot",
    "StaticFeedRef",
    "Stop",
    "StopKeyField",
    "SyncResult",
    "async_create_provider",
    "async_list_provider_keys",
    "create_provider",
    "list_provider_keys",
    "raise_for_status",
]
