"""
Addendum to transform_logic.py: pure parsing logic for the YouTube Data API
v3 videoCategories response. Kept in a separate file rather than editing
transform_logic.py directly, so the existing 34 tests and their file stay
untouched — this is purely additive.

Same functional-core principle as the rest of the project: this function
takes a plain dict and returns a plain list of dicts. No HTTP, no requests
library import here at all — see api_client.py for the I/O side.
"""

from __future__ import annotations
from typing import Any


def parse_category_api_response(raw_json: dict) -> list[dict[str, Any]]:
    """
    Parses a YouTube Data API v3 videoCategories response into flat rows:
    [{"category_id": "10", "category_title": "Music", "assignable": True}, ...]

    Raises ValueError on a structurally unexpected response (missing
    'items' key) rather than failing silently or crashing deep inside a
    dict-access chain — same defensive-parsing principle as
    transform_logic.validate_and_clean_row.
    """
    if "items" not in raw_json:
        raise ValueError(
            "Expected key 'items' not found in category API response — "
            "the API's response schema may have changed."
        )

    rows = []
    for item in raw_json["items"]:
        snippet = item.get("snippet", {})
        category_id = item.get("id")
        title = snippet.get("title")

        if category_id is None or title is None:
            # Skip malformed individual entries rather than failing the
            # whole batch — one bad category shouldn't block the other 31.
            continue

        rows.append({
            "category_id": str(category_id),
            "category_title": str(title).strip(),
            "assignable": bool(snippet.get("assignable", False)),
        })

    return rows
