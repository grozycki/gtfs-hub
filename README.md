# gtfs-hub

Provider-agnostic GTFS schedule library for low-memory hardware. It keeps a local SQLite copy of a schedule, sized by the lines you select rather than by the size of the feed.

Requires Python 3.13 or newer.

## Installation

```bash
pip install gtfs-hub
```

Versions follow [Semantic Versioning](https://semver.org/); below 1.0, a breaking change to the API, the provider contract or the database schema bumps the minor version. See the [changelog](CHANGELOG.md).

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

## Departures

Fetch realtime once per poll, and pass it to each departures call. A failed fetch only means passing `None`: the schedule still answers.

```python
from gtfs_hub import GtfsError, GtfsHub


async def show_departures(hub: GtfsHub, stop_key: str) -> None:
    try:
        realtime = await hub.async_fetch_realtime()
    except GtfsError:
        realtime = None
    for departure in await hub.async_departures(stop_key, realtime=realtime, limit=5):
        print(departure.line.short_name, departure.headsign, departure.expected, departure.status)
```

Departures cover the lines stored by the last sync, on every service date reaching the period, including night service past 24:00. Times are in UTC. `scheduled` is always set, while `predicted` and `delay` stay `None` without a usable update, so "no data" never reads as "on time". Stops a running trip has passed give no departure, and points where riders cannot board give none either. Realtime older than `max_age` (5 minutes by default) is ignored, and `RealtimeData.is_stale` tells a consumer so. When no snapshot covers today, `async_departures` raises `GtfsNoDataError`.

Trips in `frequencies.txt` are not supported and are left out, with a log line.

## Shapes

When a source publishes `shapes.txt`, the paths of selected lines are stored with their timetables. `async_line_shapes(key, service_date=None)` returns each distinct path of a line, with its points in order, its direction and the most common headsign of its trips. With a service date, only the snapshot serving that date counts, so other days' variants stay out. A broken `shapes.txt` costs only the paths, never the timetables. Vehicles carry the `shape` of their trip.

`locate(shape, latitude, longitude, after=None)` places a position on a path: metres along it, metres off it, and the bearing of the path there. A position more than 60 m off has no location. Pass the previous `along` of the same trip as `after`, so a path that runs one street both ways resolves to the pass ahead; without it, such a position has no location rather than a guess.

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
- `Line.url` and `Stop.url` carry `route_url` and `stop_url` when the source fills them.
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
