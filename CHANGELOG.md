# Changelog

All notable changes are listed here. Versions follow [Semantic Versioning](https://semver.org/); below 1.0, a breaking change to the API, the provider contract or the database schema bumps the minor version.

## 0.1.1 - 2026-11-14

- Adding a line no longer parses every stop time of the stored files: rows of other trips and shapes are skipped by their ID before parsing, for providers that keep rows unchanged. On GZM, adding a line to six took 27 s on a Mac and now takes 9 s.
- Stop choices start from the chosen lines' trips, and each sync that changes the database refreshes the SQLite planner statistics. On GZM, stop choices took 1.6 s and now take 0.4 s.
- Downloaded schedule files that the listing gives a version are kept next to the database, in `<database>.files/`, and read again rather than downloaded. After each successful listing, files of old versions, files gone from the listing and partial files are deleted. For GZM they take about 70 MB.
- `delete_store(path)` deletes a database together with its kept files and the leftovers of interrupted syncs, and `kept_files_dir(path)` names where the kept files are. A file that cannot be kept is still imported.

## 0.1.0 - 2026-11-11

First public release.

- Local SQLite copy of a GTFS schedule, sized by the selected lines, with snapshots, coverage checks and safe rebuilds.
- Departures joined with GTFS Realtime trip updates, including night service past 24:00, cancellations and skipped stops.
- Vehicle positions of selected lines, and service alerts.
- Route shapes of selected lines, and locating a position on a shape.
- `GenericProvider` for plain URLs, and the `gtfs_hub.providers` entry point group for provider packages.
- Typed errors deriving from `GtfsError`.
