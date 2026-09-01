"""Small dependency-free YouTube Data API client with retry/backoff."""
from __future__ import annotations

import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://www.googleapis.com/youtube/v3/videoCategories"


class APIError(RuntimeError):
    """Raised when the YouTube API cannot be called successfully."""


def _http_get(url: str, params: dict, timeout: float):
    query = urlencode(params)
    request = Request(
        f"{url}?{query}",
        headers={"Accept": "application/json", "User-Agent": "youtube-lakehouse/4.0"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body}
        return exc.code, payload
    except URLError as exc:
        raise APIError(f"YouTube API network error: {exc.reason}") from exc


def fetch_category_metadata(
    api_key: str,
    region_code: str = "US",
    *,
    get_fn=_http_get,
    max_retries: int = 4,
    timeout: float = 20.0,
    sleep_fn=time.sleep,
):
    """Fetch active YouTube video categories with exponential backoff.

    Retryable statuses are 429 and 5xx. Non-retryable 4xx responses fail fast.
    """
    if not api_key:
        raise APIError("YouTube API key is empty")

    params = {"part": "snippet", "regionCode": region_code, "key": api_key}
    last_status = None
    last_payload = None

    for attempt in range(max_retries + 1):
        try:
            status, payload = get_fn(API_URL, params, timeout)
        except TypeError:
            # Useful for very small test doubles that expose a 2-argument call.
            status, payload = get_fn(API_URL, params)
        last_status, last_payload = status, payload

        if 200 <= status < 300:
            return payload
        if status == 429 or status >= 500:
            if attempt < max_retries:
                sleep_fn(2**attempt)
                continue
            break
        raise APIError(f"YouTube API HTTP {status}: {payload}")

    raise APIError(
        f"YouTube API failed after {max_retries + 1} attempts; "
        f"last HTTP status={last_status}, response={last_payload}"
    )
