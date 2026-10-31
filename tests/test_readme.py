"""The README example runs as written."""

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from aiohttp import ClientSession

from conftest import FeedServer, base_tables, write_feed
from gtfs_hub import GenericProvider, GtfsHub, LineKey

README = Path(__file__).parent.parent / "README.md"


async def test_usage_example(
    tmp_path: Path, feed_server: FeedServer, capsys: pytest.CaptureFixture[str]
) -> None:
    block = re.search(r"```python\n(.*?)```", README.read_text(encoding="utf-8"), re.DOTALL)
    assert block is not None
    namespace: dict[str, Any] = {}
    exec(block.group(1), namespace)
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    await namespace["main"](url, tmp_path / "db")
    assert "Stop Beta" in capsys.readouterr().out


async def test_departures_example(
    tmp_path: Path,
    feed_server: FeedServer,
    session: ClientSession,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(encoding="utf-8"), re.DOTALL)
    namespace: dict[str, Any] = {}
    exec(blocks[1], namespace)
    url = feed_server.put("a.zip", write_feed(tmp_path / "a.zip", base_tables()))
    clock = lambda: datetime(2026, 11, 2, 5, 0, tzinfo=UTC)  # noqa: E731
    hub = GtfsHub(GenericProvider([url]), tmp_path / "db", session, clock=clock)
    await hub.async_sync([LineKey(3, "11")])
    await namespace["show_departures"](hub, "s1")
    assert "scheduled" in capsys.readouterr().out
