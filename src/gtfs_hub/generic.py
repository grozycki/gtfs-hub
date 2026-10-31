"""A provider configured with plain URLs."""

from collections.abc import Mapping, Sequence
from typing import override

from aiohttp import ClientSession

from .models import RealtimeEndpoints, StaticFeedRef, StopKeyField
from .provider import BaseProvider


class GenericProvider(BaseProvider):
    """Serve any standard source from URLs alone.

    Reports no capabilities and no vehicle identity, because nothing has been observed.
    """

    key = "generic"

    def __init__(
        self,
        schedule_urls: Sequence[str],
        *,
        trip_updates_url: str | None = None,
        vehicle_positions_url: str | None = None,
        alerts_url: str | None = None,
        headers: Mapping[str, str] | None = None,
        stop_key: StopKeyField = "stop_id",
        name: str = "GTFS",
        attribution: str | None = None,
    ) -> None:
        if not schedule_urls:
            raise ValueError("At least one schedule URL is required")
        self.name = name
        self.attribution = attribution
        self.stop_key = stop_key
        self._schedule_urls = tuple(schedule_urls)
        self._realtime = RealtimeEndpoints(
            trip_updates=trip_updates_url,
            vehicle_positions=vehicle_positions_url,
            alerts=alerts_url,
            headers=dict(headers or {}),
        )

    async def async_list_static_feeds(self, session: ClientSession) -> list[StaticFeedRef]:
        """Return one reference per configured URL."""
        return [StaticFeedRef(url=url) for url in self._schedule_urls]

    @override
    def realtime_endpoints(self) -> RealtimeEndpoints:
        """Return the configured endpoints."""
        return self._realtime
