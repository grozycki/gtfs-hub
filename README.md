# gtfs-hub

Provider-agnostic GTFS schedule library for low-memory hardware. It keeps a local SQLite copy of a schedule, sized by the lines you select rather than by the size of the feed.

Requires Python 3.13 or newer.

## Usage

Any standard source works from plain URLs, through `GenericProvider`. Pass your own `aiohttp` session: the library never creates one.

```python
from pathlib import Path

from aiohttp import ClientSession

from gtfs_hub import GenericProvider, GtfsHub, LineKey


async def main(url: str, database: Path) -> None:
    async with ClientSession() as session:
        hub = GtfsHub(GenericProvider([url]), database, session)

        # The first sync with no lines fills the searchable catalogue.
        await hub.async_sync([])
        lines = await hub.async_search_lines("11")

        # Timetables are stored only for selected lines.
        selection = [line.key for line in lines]
        await hub.async_sync(selection)

        for key in selection:
            stops = await hub.async_stops_of_line(key)
            print(LineKey(key.route_type, key.short_name), [stop.name for stop in stops])
```

Call `async_sync` with the full selection whenever the source may have changed or the selection did. Lines that are new are imported, lines that are gone are removed, and unchanged files are not downloaded. On failure it raises a `GtfsError`, and the previous database stays in use. The database is a cache, so it is safe to delete: the next sync rebuilds it.

## Generic provider

| Argument | Meaning |
|---|---|
| `schedule_urls` | One or more ZIP files, each treated as one snapshot |
| `trip_updates_url`, `vehicle_positions_url`, `alerts_url` | Optional realtime endpoints |
| `headers` | Optional headers for realtime requests |
| `stop_key` | `stop_id` by default, or `stop_code` when that is the stable field |
| `name`, `attribution` | Display name, and the licence text consumers must show |

It reports no capabilities and no vehicle identity until a source has been surveyed.

## Keys

- **Lines** are `LineKey(route_type, short_name)`. Store this, never a `route_id`.
- **Stops** are the value of the provider's `stop_key` field. One key can resolve to several `stop_id` values.
- Empty results from `async_resolve_line` and `async_resolve_stop` mean the key is in no current snapshot.

## Coverage

`async_snapshot_for(date)` picks the newest snapshot for a service date. `async_missing_dates(start, end)` reports dates no snapshot covers, which differ from dates without service.

## Errors

| Error | Meaning |
|---|---|
| `GtfsConnectionError` | Temporary failure: retry later |
| `GtfsRateLimitError` | Slow down, with `retry_after` in seconds when known |
| `GtfsAuthError` | The source rejected the credentials |
| `GtfsFeedError` | Bad or unusable data |
| `GtfsNoDataError` | No usable database yet: run a sync |

All derive from `GtfsError`.

## Writing a provider

A package is needed only when finding or reading the data takes code. Subclass `BaseProvider`, implement `async_list_static_feeds`, and register a factory in the `gtfs_hub.providers` entry point group, with the provider key as its name.

## Development

```bash
uv sync
```

```bash
uv run pytest
```
