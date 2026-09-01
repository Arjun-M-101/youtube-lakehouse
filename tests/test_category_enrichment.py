import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from category_enrichment import parse_category_api_response


def test_valid_response():
    raw={"items":[{"id":"10","snippet":{"title":" Music ","assignable":True}}]}
    assert parse_category_api_response(raw)==[{"category_id":"10","category_title":"Music","assignable":True}]


def test_multiple_entries():
    raw={"items":[{"id":"10","snippet":{"title":"Music"}},{"id":"20","snippet":{"title":"Gaming"}}]}
    assert len(parse_category_api_response(raw))==2


def test_missing_items_raises():
    with pytest.raises(ValueError, match="Expected key 'items'"):
        parse_category_api_response({})


def test_empty_items():
    assert parse_category_api_response({"items":[]})==[]


def test_missing_id_skipped():
    assert parse_category_api_response({"items":[{"snippet":{"title":"Music"}},{"id":"20","snippet":{"title":"Gaming"}}]}) == [{"category_id":"20","category_title":"Gaming","assignable":False}]


def test_missing_title_skipped():
    assert parse_category_api_response({"items":[{"id":"10","snippet":{}},{"id":"20","snippet":{"title":"Gaming"}}]})[0]["category_id"] == "20"


def test_assignable_defaults_false():
    row=parse_category_api_response({"items":[{"id":"10","snippet":{"title":"Music"}}]})[0]
    assert row["assignable"] is False


def test_integer_id_coerced_to_string():
    row=parse_category_api_response({"items":[{"id":10,"snippet":{"title":"Music"}}]})[0]
    assert row["category_id"] == "10"
