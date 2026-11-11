# Changelog

All notable changes are listed here. Versions follow [Semantic Versioning](https://semver.org/); below 1.0, a breaking change to the API, the provider contract or the database schema bumps the minor version.

## 0.1.0 - 2026-11-11

First public release.

- Local SQLite copy of a GTFS schedule, sized by the selected lines, with snapshots, coverage checks and safe rebuilds.
- Departures joined with GTFS Realtime trip updates, including night service past 24:00, cancellations and skipped stops.
- Vehicle positions of selected lines, and service alerts.
- Route shapes of selected lines, and locating a position on a shape.
- `GenericProvider` for plain URLs, and the `gtfs_hub.providers` entry point group for provider packages.
- Typed errors deriving from `GtfsError`.
