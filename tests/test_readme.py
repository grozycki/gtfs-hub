"""The README example runs as written."""

import re
from pathlib import Path
from typing import Any

import pytest

from conftest import FeedServer, base_tables, write_feed

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
