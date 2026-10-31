"""Rules for choosing and expiring snapshots."""

from collections.abc import Iterable
from datetime import date, datetime, time, timedelta

from .models import Snapshot

DAY_SECONDS = 86_400
# Assumed length of a service day when no stop times were read for a snapshot.
UNKNOWN_SERVICE_DAY_SECONDS = 30 * 3_600


def select_snapshot(snapshots: Iterable[Snapshot], service_date: date) -> Snapshot | None:
    """Return the newest snapshot covering a service date."""
    matching = [s for s in snapshots if s.valid_from <= service_date <= s.valid_to]
    return max(matching, key=lambda s: s.id, default=None)


def missing_dates(snapshots: Iterable[Snapshot], start: date, end: date) -> list[date]:
    """Return the dates of a range that no snapshot covers."""
    covered = list(snapshots)
    days = (start + timedelta(days=n) for n in range((end - start).days + 1))
    return [day for day in days if select_snapshot(covered, day) is None]


def is_expired(valid_to: date, max_seconds: int | None, now_local: datetime) -> bool:
    """Tell whether the last service day of a window can have no running trips."""
    seconds = UNKNOWN_SERVICE_DAY_SECONDS if max_seconds is None else max_seconds
    last_trip_ends = datetime.combine(valid_to, time()) + timedelta(
        seconds=max(seconds, DAY_SECONDS)
    )
    return now_local.replace(tzinfo=None) >= last_trip_ends
