"""Errors raised by the library."""


class GtfsError(Exception):
    """Base class for all library errors."""


class GtfsConnectionError(GtfsError):
    """Temporary failure: retry later."""


class GtfsRateLimitError(GtfsError):
    """The source asked to slow down."""

    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class GtfsAuthError(GtfsError):
    """The source rejected the credentials."""


class GtfsFeedError(GtfsError):
    """The data is bad or unusable: keep the last good data."""


class GtfsNoDataError(GtfsError):
    """No stored schedule can answer the request."""
