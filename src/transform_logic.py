"""Pure YouTube row validation, normalization, deduplication and aggregation."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import math
import re
from typing import Any, Iterable, Mapping

REQUIRED_FIELDS = (
    "video_id",
    "title",
    "channel_title",
    "category_id",
    "trending_date",
    "views",
    "likes",
    "comment_count",
)
DUPLICATE_ROW = "DUPLICATE_ROW"


class ValidationResult:
    def __init__(self, row: dict, reason: str | None = None):
        self.row = row
        self.reason = reason

    @property
    def valid(self) -> bool:
        return self.reason is None


def parse_int_safe(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        if isinstance(value, bool):
            return None
        number = float(str(value).replace(",", "").strip())
        if not math.isfinite(number) or number != int(number):
            return None
        return int(number)
    except (TypeError, ValueError):
        return None


def parse_bool_safe(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y", "t"}:
        return True
    if token in {"false", "0", "no", "n", "f"}:
        return False
    return default


def parse_publish_time(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    candidates = [raw, raw.replace("Z", "+00:00")]
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate).isoformat()
        except ValueError:
            continue
    return None


def parse_trending_date(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    for fmt in ("%y.%d.%m", "%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


KNOWN_REGIONS = {"US", "GB", "CA", "DE", "FR", "IN", "JP", "KR", "MX", "RU"}

def extract_region_from_filename(path: str) -> str | None:
    """Accepts USvideos.csv, US_production_run.csv, US_final_run.csv, etc."""
    filename = str(path).rstrip("/").split("/")[-1]
    match = re.match(r"^([A-Za-z]{2})", filename)
    if match and match.group(1).upper() in KNOWN_REGIONS:
        return match.group(1).upper()
    return None


def validate_and_clean_row(row: Mapping[str, Any], region: str | None) -> ValidationResult:
    source = dict(row)

    for field in REQUIRED_FIELDS:
        if field not in source or source[field] is None or str(source[field]).strip() == "":
            return ValidationResult(source, "MISSING_REQUIRED_FIELD")

    category_id = parse_int_safe(source["category_id"])
    views = parse_int_safe(source["views"])
    likes = parse_int_safe(source["likes"])
    comment_count = parse_int_safe(source["comment_count"])
    trending_date = parse_trending_date(source["trending_date"])

    if any(value is None for value in (category_id, views, likes, comment_count)):
        return ValidationResult(source, "INVALID_NUMERIC_FIELD")
    if any(value < 0 for value in (views, likes, comment_count)):
        return ValidationResult(source, "NEGATIVE_METRIC")
    if trending_date is None:
        return ValidationResult(source, "BAD_DATE")
    if region is None:
        return ValidationResult(source, "UNKNOWN_REGION")

    clean = {
        "video_id": str(source["video_id"]).strip(),
        "title": str(source["title"]).strip(),
        "channel_title": str(source["channel_title"]).strip(),
        "category_id": category_id,
        "trending_date": trending_date,
        "views": views,
        "likes": likes,
        "comment_count": comment_count,
        "region": region,
        "dislikes": max(parse_int_safe(source.get("dislikes")) or 0, 0),
        "publish_time": parse_publish_time(source.get("publish_time")),
        "comments_disabled": parse_bool_safe(source.get("comments_disabled")),
        "ratings_disabled": parse_bool_safe(source.get("ratings_disabled")),
        "video_error_or_removed": parse_bool_safe(source.get("video_error_or_removed")),
    }
    clean["engagement_ratio"] = round(
        (clean["likes"] + clean["comment_count"]) / clean["views"], 6
    ) if clean["views"] > 0 else 0.0
    return ValidationResult(clean)


def dedupe_silver_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict]:
    seen: set[tuple[Any, ...]] = set()
    output: list[dict] = []
    for row in rows:
        key = (row.get("video_id"), row.get("trending_date"), row.get("region"))
        if key in seen:
            continue
        seen.add(key)
        output.append(dict(row))
    return output


def dedupe_silver_rows_with_rejections(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict], list[dict]]:
    seen: set[tuple[Any, ...]] = set()
    accepted: list[dict] = []
    duplicates: list[dict] = []
    for row in rows:
        clean = dict(row)
        key = (clean.get("video_id"), clean.get("trending_date"), clean.get("region"))
        if key in seen:
            clean["quarantine_reason"] = DUPLICATE_ROW
            duplicates.append(clean)
            continue
        seen.add(key)
        accepted.append(clean)
    return accepted, duplicates


def build_category_daily_summary(rows: Iterable[Mapping[str, Any]], category_lookup: Mapping[int, str]) -> list[dict]:
    buckets: dict[tuple[int, str, str], dict[str, float]] = defaultdict(
        lambda: {"video_count": 0, "total_views": 0, "total_likes": 0, "total_dislikes": 0, "total_comments": 0, "engagement_sum": 0.0}
    )
    for row in rows:
        key = (int(row["category_id"]), str(row["trending_date"]), str(row["region"]))
        bucket = buckets[key]
        bucket["video_count"] += 1
        bucket["total_views"] += int(row["views"])
        bucket["total_likes"] += int(row["likes"])
        bucket["total_dislikes"] += int(row.get("dislikes", 0))
        bucket["total_comments"] += int(row["comment_count"])
        bucket["engagement_sum"] += float(row.get("engagement_ratio", 0.0))

    output = []
    for (category_id, trending_date, region), bucket in sorted(buckets.items()):
        count = bucket["video_count"]
        total_views = bucket["total_views"]
        output.append({
            "category_id": category_id,
            "category_name": category_lookup.get(category_id, f"Unknown ({category_id})"),
            "trending_date": trending_date,
            "region": region,
            "video_count": count,
            "total_views": total_views,
            "total_likes": bucket["total_likes"],
            "total_dislikes": bucket["total_dislikes"],
            "total_comments": bucket["total_comments"],
            "avg_views_per_video": round(total_views / count, 2),
            "avg_engagement_ratio": round(bucket["engagement_sum"] / count, 6),
        })
    return output


def data_quality_report(total_rows: int, clean_rows: int, rejected_rows: Iterable[Mapping[str, Any]], duplicate_rows: int, threshold: float = 0.95) -> dict:
    reason_counts = Counter(r.get("quarantine_reason") for r in rejected_rows)
    valid_rows = clean_rows + duplicate_rows
    validity_rate = (valid_rows / total_rows) if total_rows else 0.0
    duplicate_rate = (duplicate_rows / total_rows) if total_rows else 0.0
    pass_rate = (clean_rows / total_rows) if total_rows else 0.0
    return {
        "total_bronze_rows": total_rows,
        "validated_clean_rows": clean_rows,
        "quarantined_rows": total_rows - clean_rows - duplicate_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_rate": round(duplicate_rate, 6),
        "pass_rate": round(pass_rate, 6),
        "validity_rate": round(validity_rate, 6),
        "threshold": threshold,
        "pass": validity_rate >= threshold,
        "reasons": dict(reason_counts),
    }
