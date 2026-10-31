"""Find installed provider packages through entry points."""

import asyncio
from importlib.metadata import entry_points

from .errors import GtfsError
from .provider import Provider

ENTRY_POINT_GROUP = "gtfs_hub.providers"


def list_provider_keys() -> list[str]:
    """Return the keys of installed providers. Touches the disk."""
    return sorted(point.name for point in entry_points(group=ENTRY_POINT_GROUP))


def create_provider(key: str) -> Provider:
    """Create an installed provider from its key. Touches the disk."""
    for point in entry_points(group=ENTRY_POINT_GROUP):
        if point.name == key:
            provider: Provider = point.load()()
            return provider
    raise GtfsError(f"Provider not installed: {key}")


async def async_list_provider_keys() -> list[str]:
    """Return the keys of installed providers."""
    return await asyncio.get_running_loop().run_in_executor(None, list_provider_keys)


async def async_create_provider(key: str) -> Provider:
    """Create an installed provider from its key."""
    return await asyncio.get_running_loop().run_in_executor(None, create_provider, key)
