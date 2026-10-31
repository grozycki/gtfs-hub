"""Download schedule files to disk in chunks."""

import asyncio
import hashlib
from pathlib import Path

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

from .errors import GtfsAuthError, GtfsConnectionError, GtfsFeedError, GtfsRateLimitError

_CHUNK_SIZE = 64 * 1024
# Large files are slow, so limit stalls rather than the whole transfer.
_TIMEOUT = ClientTimeout(total=None, sock_connect=30, sock_read=60)


def raise_for_status(response: ClientResponse, url: str) -> None:
    """Turn a failed response into the matching error."""
    status = response.status
    if status == 429:
        header = response.headers.get("Retry-After", "")
        retry_after = int(header) if header.isdecimal() else None
        raise GtfsRateLimitError(f"Rate limited by {url}", retry_after=retry_after)
    if status in (401, 403):
        raise GtfsAuthError(f"Access denied by {url}: {status}")
    if status >= 500:
        raise GtfsConnectionError(f"Server error from {url}: {status}")
    if status >= 400:
        raise GtfsFeedError(f"Cannot fetch {url}: {status}")


async def async_download(
    session: ClientSession, url: str, destination: Path, timeout: ClientTimeout = _TIMEOUT
) -> str:
    """Stream a URL into a file and return its SHA-256."""
    loop = asyncio.get_running_loop()
    digest = hashlib.sha256()
    target = await loop.run_in_executor(None, destination.open, "wb")
    try:
        async with session.get(url, timeout=timeout) as response:
            raise_for_status(response, url)
            async for chunk in response.content.iter_chunked(_CHUNK_SIZE):
                digest.update(chunk)
                await loop.run_in_executor(None, target.write, chunk)
    except (ClientError, TimeoutError) as err:
        raise GtfsConnectionError(f"Cannot fetch {url}: {err}") from err
    finally:
        await loop.run_in_executor(None, target.close)
    return digest.hexdigest()
