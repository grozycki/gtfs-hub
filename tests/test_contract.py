"""Models, errors, the provider contract and discovery."""

import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession

from gtfs_hub import (
    BaseProvider,
    Capabilities,
    GenericProvider,
    GtfsAuthError,
    GtfsConnectionError,
    GtfsError,
    GtfsFeedError,
    GtfsNoDataError,
    GtfsRateLimitError,
    LineKey,
    Provider,
    RealtimeEndpoints,
    StaticFeedRef,
    async_list_provider_keys,
    create_provider,
    discovery,
    list_provider_keys,
)

ROOT = Path(__file__).parent.parent


class Minimal(BaseProvider):
    key = "minimal"
    name = "Minimal"

    async def async_list_static_feeds(self, session: ClientSession) -> list[StaticFeedRef]:
        return []


@pytest.mark.parametrize(
    "error",
    [GtfsConnectionError, GtfsRateLimitError, GtfsAuthError, GtfsFeedError, GtfsNoDataError],
)
def test_errors_share_a_base(error: type[Exception]) -> None:
    assert issubclass(error, GtfsError)


def test_rate_limit_carries_its_delay() -> None:
    assert GtfsRateLimitError("slow", retry_after=30).retry_after == 30
    assert GtfsRateLimitError("slow").retry_after is None


def test_models_compare_by_value_and_are_frozen() -> None:
    assert LineKey(3, "11") == LineKey(3, "11")
    assert LineKey(3, "11") != LineKey(0, "11")
    with pytest.raises(FrozenInstanceError):
        LineKey(3, "11").short_name = "12"  # type: ignore[misc]  # the assignment must fail


def test_absent_values_stay_none() -> None:
    ref = StaticFeedRef("http://example.test/a.zip")
    assert (ref.version, ref.valid_from, ref.valid_to) == (None, None, None)
    endpoints = RealtimeEndpoints()
    assert (endpoints.trip_updates, endpoints.alerts) == (None, None)


def test_base_provider_defaults() -> None:
    provider = Minimal()
    assert isinstance(provider, Provider)
    row = {"stop_id": " raw "}
    assert provider.normalize_static("stops", row) == {"stop_id": " raw "}
    assert provider.vehicle_key("vehicle-1", "label", "trip") is None
    assert provider.vehicle_label("vehicle-1", "1347") == "1347"
    assert provider.vehicle_label("vehicle-1", "") is None
    assert provider.realtime_endpoints().alerts is None
    assert provider.stop_key == "stop_id"


def test_undeclared_capability_reads_absent() -> None:
    assert Minimal().capabilities == Capabilities()
    assert Capabilities(bearing=True).speed is False


def _fake_points(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    points = [SimpleNamespace(name=name, load=lambda: Minimal) for name in names]
    monkeypatch.setattr(discovery, "entry_points", lambda group: points)


def test_discovery_finds_installed_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_points(monkeypatch, "minimal")
    assert list_provider_keys() == ["minimal"]
    assert create_provider("minimal").key == "minimal"


async def test_discovery_async(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_points(monkeypatch, "minimal")
    assert await async_list_provider_keys() == ["minimal"]


def test_core_works_with_no_provider_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_points(monkeypatch)
    assert list_provider_keys() == []
    with pytest.raises(GtfsError):
        create_provider("missing")
    assert GenericProvider(["http://example.test/a.zip"]).key == "generic"


def test_agnostic_check_fails_on_a_planted_name(tmp_path: Path) -> None:
    script = ROOT / "scripts" / "check_agnostic.py"
    (tmp_path / "ok.py").write_text("x = 1\n")
    assert subprocess.run([sys.executable, script, tmp_path], check=False).returncode == 0
    (tmp_path / "bad.py").write_text('url = "https://transportGZM.example"\n')
    assert subprocess.run([sys.executable, script, tmp_path], check=False).returncode == 1


def test_core_source_is_agnostic() -> None:
    script = ROOT / "scripts" / "check_agnostic.py"
    source = ROOT / "src" / "gtfs_hub"
    assert subprocess.run([sys.executable, script, source], check=False).returncode == 0
