"""Locate positions on shapes. Distances use a flat projection, exact enough within a city."""

import math

from .models import Shape, ShapeLocation

_EARTH_RADIUS = 6_371_000.0
# Farther than this, a vehicle is off its path, on a detour or in a depot.
MAX_OFFSET = 60.0
# Vehicles move forward, so a location this far behind the previous one is another pass.
BACKTRACK = 50.0
# Passes this close in distance off are a tie, which only the order along the path breaks.
TIE = 20.0
_OPPOSED = 90.0


def _angle(first: float, second: float) -> float:
    difference = abs(first - second) % 360
    return min(difference, 360 - difference)


def _passes(shape: Shape, latitude: float, longitude: float) -> list[ShapeLocation]:
    """Return the nearest location of each stretch of the path within reach, in path order."""
    scale = math.cos(math.radians(latitude)) * _EARTH_RADIUS

    def flat(point: tuple[float, float]) -> tuple[float, float]:
        return (
            math.radians(point[1] - longitude) * scale,
            math.radians(point[0] - latitude) * _EARTH_RADIUS,
        )

    passes: list[ShapeLocation] = []
    current: ShapeLocation | None = None
    along = 0.0
    start = flat(shape.points[0])
    for point in shape.points[1:]:
        end = flat(point)
        dx, dy = end[0] - start[0], end[1] - start[1]
        length = math.hypot(dx, dy)
        if length > 0:
            share = max(0.0, min(1.0, -(start[0] * dx + start[1] * dy) / length**2))
            offset = math.hypot(start[0] + share * dx, start[1] + share * dy)
            if offset <= MAX_OFFSET:
                found = ShapeLocation(
                    along + share * length, offset, math.degrees(math.atan2(dx, dy)) % 360
                )
                if current is None or found.offset < current.offset:
                    current = found
            elif current is not None:
                passes.append(current)
                current = None
            along += length
        start = end
    if current is not None:
        passes.append(current)
    return passes


def locate(
    shape: Shape, latitude: float, longitude: float, after: float | None = None
) -> ShapeLocation | None:
    """Locate a position on a shape, or return None when it is off the path or ambiguous.

    `after` is the previous distance along the same trip. Passes behind it lose, and among
    tied passes ahead the first wins, as vehicles move forward. Without it, tied passes
    pointing opposite ways leave the position unlocated.
    """
    if len(shape.points) < 2:
        return None
    passes = _passes(shape, latitude, longitude)
    if after is not None:
        passes = [found for found in passes if found.along >= after - BACKTRACK] or passes
    if not passes:
        return None
    nearest = min(passes, key=lambda found: found.offset)
    tied = [found for found in passes if found.offset <= nearest.offset + TIE]
    if after is not None:
        return tied[0]
    if any(_angle(found.bearing, nearest.bearing) > _OPPOSED for found in tied):
        return None
    return nearest
