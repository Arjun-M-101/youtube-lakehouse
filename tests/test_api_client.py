import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from api_client import APIError, API_URL, fetch_category_metadata


def fake_get_factory(responses):
    calls = []
    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        value = responses[min(len(calls)-1, len(responses)-1)]
        if isinstance(value, Exception):
            raise value
        return value
    fake_get.calls = calls
    return fake_get


def test_success():
    fn = fake_get_factory([(200, {"items": []})])
    assert fetch_category_metadata("key", get_fn=fn) == {"items": []}
    assert len(fn.calls) == 1
    assert fn.calls[0][0] == API_URL


def test_region_parameter_is_sent():
    fn = fake_get_factory([(200, {"items": []})])
    fetch_category_metadata("key", region_code="GB", get_fn=fn)
    assert fn.calls[0][1]["regionCode"] == "GB"


def test_429_retries():
    fn = fake_get_factory([(429, {}), (200, {"items": [1]})])
    assert fetch_category_metadata("key", get_fn=fn, sleep_fn=lambda _: None) == {"items": [1]}
    assert len(fn.calls) == 2


def test_500_retries():
    fn = fake_get_factory([(500, {}), (500, {}), (200, {"items": []})])
    assert fetch_category_metadata("key", get_fn=fn, sleep_fn=lambda _: None) == {"items": []}
    assert len(fn.calls) == 3


def test_nonretryable_400_fails_fast():
    fn = fake_get_factory([(400, {"error": "bad request"}), (200, {})])
    with pytest.raises(APIError, match="HTTP 400"):
        fetch_category_metadata("key", get_fn=fn, sleep_fn=lambda _: None)
    assert len(fn.calls) == 1


def test_nonretryable_403_fails_fast():
    fn = fake_get_factory([(403, {"error": "forbidden"})])
    with pytest.raises(APIError, match="HTTP 403"):
        fetch_category_metadata("key", get_fn=fn)
    assert len(fn.calls) == 1


def test_exhausts_retries():
    fn = fake_get_factory([(500, {})])
    with pytest.raises(APIError, match="failed after 3 attempts"):
        fetch_category_metadata("key", get_fn=fn, max_retries=2, sleep_fn=lambda _: None)
    assert len(fn.calls) == 3


def test_empty_key_fails():
    fn = fake_get_factory([(200, {})])
    with pytest.raises(APIError, match="key is empty"):
        fetch_category_metadata("", get_fn=fn)
