import pytest
from aiohttp import ClientSession

from gtfs_hub import Capabilities, GenericProvider


async def test_one_url_lists_one_file(session: ClientSession) -> None:
    provider = GenericProvider(["http://example.test/a.zip"])
    refs = await provider.async_list_static_feeds(session)
    assert [ref.url for ref in refs] == ["http://example.test/a.zip"]


async def test_several_urls(session: ClientSession) -> None:
    provider = GenericProvider(["http://example.test/a.zip", "http://example.test/b.zip"])
    assert len(await provider.async_list_static_feeds(session)) == 2


def test_no_realtime_is_not_an_error() -> None:
    endpoints = GenericProvider(["http://example.test/a.zip"]).realtime_endpoints()
    assert endpoints.trip_updates is None
    assert endpoints.vehicle_positions is None
    assert endpoints.alerts is None


def test_realtime_urls_and_headers() -> None:
    endpoints = GenericProvider(
        ["http://example.test/a.zip"],
        trip_updates_url="http://example.test/tu",
        headers={"Key": "value"},
    ).realtime_endpoints()
    assert endpoints.trip_updates == "http://example.test/tu"
    assert endpoints.headers == {"Key": "value"}


def test_conservative_defaults() -> None:
    provider = GenericProvider(["http://example.test/a.zip"])
    assert provider.stop_key == "stop_id"
    assert provider.capabilities == Capabilities()
    assert provider.vehicle_key("v1", "label", "t1") is None


def test_stop_key_is_configurable() -> None:
    provider = GenericProvider(["http://example.test/a.zip"], stop_key="stop_code")
    assert provider.stop_key == "stop_code"


def test_requires_a_url() -> None:
    with pytest.raises(ValueError, match="schedule URL"):
        GenericProvider([])
