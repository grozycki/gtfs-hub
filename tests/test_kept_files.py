"""Schedule files kept between syncs, and their cleanup."""

import sqlite3
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from aiohttp import ClientSession

from conftest import FeedServer, base_tables, write_feed
from gtfs_hub import (
    BaseProvider,
    GenericProvider,
    GtfsConnectionError,
    GtfsFeedError,
    GtfsHub,
    LineKey,
    StaticFeedRef,
    delete_store,
    kept_files_dir,
)

LINE_11 = LineKey(3, "11")
LINE_12 = LineKey(3, "12")


class Listed(BaseProvider):
    """A provider whose listing tests set directly."""

    key = "listed"
    name = "Listed"

    def __init__(self, refs: list[StaticFeedRef]) -> None:
        self.refs = refs
        self.failure: Exception | None = None

    async def async_list_static_feeds(self, session: ClientSession) -> list[StaticFeedRef]:
        if self.failure:
            raise self.failure
        return self.refs


def kept(path: Path) -> list[Path]:
    directory = kept_files_dir(path)
    return sorted(directory.iterdir()) if directory.is_dir() else []


async def test_adding_a_line_reads_the_kept_file(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    assert feed_server.total_hits() == 1
    assert len(kept(path)) == 1
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    assert feed_server.total_hits() == 1
    assert await hub.async_stops_of_line(LINE_12)


async def test_deleted_database_is_rebuilt_from_kept_files(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    path.unlink()
    assert (await hub.async_sync([LINE_11])).imported == 1
    assert feed_server.total_hits() == 1


async def test_file_without_a_version_is_not_kept(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    store = tmp_path / "store"
    path = store / "db"
    hub = GtfsHub(GenericProvider([url]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    await hub.async_sync([LINE_11, LINE_12])
    assert feed_server.total_hits() == 2
    assert [item.name for item in store.iterdir()] == ["db"]


async def test_damaged_kept_file_is_downloaded_again(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    (file,) = kept(path)
    file.write_bytes(file.read_bytes()[: file.stat().st_size // 2])
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    assert feed_server.total_hits() == 2
    assert kept(path) == [file]
    assert file.read_bytes() == feed_server.files["a.zip"]


async def test_new_version_replaces_the_kept_file(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, path, session, clock=now)
    await hub.async_sync([LINE_11])
    (old,) = kept(path)
    provider.refs = [StaticFeedRef(url, version="v2")]
    await hub.async_sync([LINE_11])
    (new,) = kept(path)
    assert new != old
    assert feed_server.total_hits() == 2


async def test_delisted_file_is_deleted(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    one = feed_server.put("1.zip", write_feed(tmp_path / "1.zip", base_tables()))
    two = feed_server.put(
        "2.zip", write_feed(tmp_path / "2.zip", base_tables(start="20261108", end="20261114"))
    )
    path = tmp_path / "db"
    provider = Listed([StaticFeedRef(one, version="v1"), StaticFeedRef(two, version="v2")])
    hub = GtfsHub(provider, path, session, clock=now)
    await hub.async_sync([LINE_11])
    assert len(kept(path)) == 2
    provider.refs = provider.refs[:1]
    await hub.async_sync([LINE_11])
    assert len(kept(path)) == 1
    # Nothing listed covers the second window, so its snapshot stays while its file goes.
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 2
    conn.close()


async def test_file_that_cannot_be_kept_still_imports(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    kept_files_dir(path).write_bytes(b"not a directory")
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    assert (await hub.async_sync([LINE_11])).imported == 1
    assert await hub.async_stops_of_line(LINE_11)
    assert [item.name for item in tmp_path.iterdir() if item.name.startswith(".db")] == []


async def test_directory_at_a_kept_name_is_replaced(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, path, session, clock=now)
    await hub.async_sync([LINE_11])
    (file,) = kept(path)
    file.unlink()
    (file / "inner").mkdir(parents=True)
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    assert file.is_file()


async def test_stray_and_partial_files_are_deleted(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    directory = kept_files_dir(path)
    (directory / "old.zip.part").write_bytes(b"partial")
    (directory / "leftover").mkdir()
    await hub.async_sync([LINE_11])
    assert len(kept(path)) == 1


async def test_kept_files_are_pruned_when_a_later_step_fails(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, path, session, clock=now)
    await hub.async_sync([LINE_11])
    provider.refs = [StaticFeedRef(feed_server.url("missing.zip"), version="v2")]
    with pytest.raises(GtfsFeedError):
        await hub.async_sync([LINE_11])
    assert kept(path) == []
    assert await hub.async_stops_of_line(LINE_11)


async def test_failed_listing_leaves_kept_files(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    provider = Listed([StaticFeedRef(url, version="v1")])
    hub = GtfsHub(provider, path, session, clock=now)
    await hub.async_sync([LINE_11])
    before = kept(path)
    provider.failure = GtfsConnectionError("down")
    with pytest.raises(GtfsConnectionError):
        await hub.async_sync([LINE_11, LINE_12])
    assert kept(path) == before


async def test_delete_store_removes_everything(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    store = tmp_path / "store"
    path = store / "db"
    await GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now).async_sync(
        [LINE_11]
    )
    (store / ".db.leftover").mkdir()
    (store / "other.db").write_bytes(b"")
    delete_store(path)
    assert [item.name for item in store.iterdir()] == ["other.db"]


def test_delete_store_with_nothing_there(tmp_path: Path) -> None:
    delete_store(tmp_path / "absent" / "db")
    delete_store(tmp_path / "db")
    assert list(tmp_path.iterdir()) == []


async def test_kept_file_that_fails_to_import_is_dropped(
    tmp_path: Path, feed_server: FeedServer, session: ClientSession, now: Callable[[], datetime]
) -> None:
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    path = tmp_path / "db"
    hub = GtfsHub(Listed([StaticFeedRef(url, version="v1")]), path, session, clock=now)
    await hub.async_sync([LINE_11])
    (file,) = kept(path)
    broken = base_tables(stop_times=[("t2", "bad", "bad", "s2", 0)])
    write_feed(file, broken)
    with pytest.raises(GtfsFeedError):
        await hub.async_sync([LINE_11, LINE_12])
    assert kept(path) == []
    assert (await hub.async_sync([LINE_11, LINE_12])).extended == 1
    assert feed_server.total_hits() == 2
