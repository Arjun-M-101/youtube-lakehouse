import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from transform_logic import (
    REQUIRED_FIELDS,
    DUPLICATE_ROW,
    ValidationResult,
    build_category_daily_summary,
    data_quality_report,
    dedupe_silver_rows,
    dedupe_silver_rows_with_rejections,
    extract_region_from_filename,
    parse_bool_safe,
    parse_int_safe,
    parse_publish_time,
    parse_trending_date,
    validate_and_clean_row,
)

GOOD_ROW = {
    "video_id": "abc123",
    "title": "  My Video  ",
    "channel_title": " Some Channel ",
    "category_id": "24",
    "trending_date": "17.14.11",
    "views": "1,000",
    "likes": "100",
    "comment_count": "10",
}


def test_required_fields_are_exactly_eight():
    assert len(REQUIRED_FIELDS) == 8


def test_validation_result_defaults_valid():
    result = ValidationResult({})
    assert result.valid is True
    assert result.reason is None


def test_parse_int_plain():
    assert parse_int_safe(42) == 42


def test_parse_int_comma_string():
    assert parse_int_safe("1,234") == 1234


def test_parse_int_whitespace():
    assert parse_int_safe(" 42 ") == 42


def test_parse_int_float_integer():
    assert parse_int_safe(42.0) == 42


def test_parse_int_rejects_fraction():
    assert parse_int_safe(42.9) is None


def test_parse_int_none():
    assert parse_int_safe(None) is None


def test_parse_int_blank():
    assert parse_int_safe(" ") is None


def test_parse_int_garbage():
    assert parse_int_safe("abc") is None


def test_parse_int_bool_rejected():
    assert parse_int_safe(True) is None


def test_parse_int_non_finite_rejected():
    assert parse_int_safe("nan") is None


def test_date_kaggle():
    assert parse_trending_date("17.14.11") == "2017-11-14"


def test_date_iso():
    assert parse_trending_date("2017-11-14") == "2017-11-14"


def test_date_slash():
    assert parse_trending_date("2017/11/14") == "2017-11-14"


def test_date_iso_datetime():
    assert parse_trending_date("2017-11-14T08:00:00") == "2017-11-14"


def test_date_invalid_day():
    assert parse_trending_date("17.99.11") is None


def test_date_garbage():
    assert parse_trending_date("banana") is None


def test_date_none():
    assert parse_trending_date(None) is None


def test_region_plain():
    assert extract_region_from_filename("USvideos.csv") == "US"


def test_region_s3_path():
    assert extract_region_from_filename("s3://bucket/bronze/youtube/CAvideos.csv") == "CA"


def test_region_uppercases_lowercase_country():
    assert extract_region_from_filename("inVideos.csv") == "IN"


def test_region_nonmatching():
    assert extract_region_from_filename("trending.csv") is None


def test_region_none():
    assert extract_region_from_filename(None) is None


def test_bool_true():
    assert parse_bool_safe("True") is True


def test_bool_false():
    assert parse_bool_safe("False") is False


def test_bool_yes():
    assert parse_bool_safe("yes") is True


def test_bool_no():
    assert parse_bool_safe("no") is False


def test_bool_missing_default():
    assert parse_bool_safe(None) is False


def test_bool_custom_default():
    assert parse_bool_safe(None, default=True) is True


def test_bool_native():
    assert parse_bool_safe(True) is True


def test_bool_garbage_default():
    assert parse_bool_safe("maybe") is False


def test_publish_z_millis():
    assert parse_publish_time("2017-11-10T17:00:03.000Z") == "2017-11-10T17:00:03+00:00"


def test_publish_z_no_millis():
    assert parse_publish_time("2017-11-10T17:00:03Z") == "2017-11-10T17:00:03+00:00"


def test_publish_iso_offset():
    assert parse_publish_time("2017-11-10T17:00:03+00:00") == "2017-11-10T17:00:03+00:00"


def test_publish_bad():
    assert parse_publish_time("not-a-time") is None


def test_publish_none():
    assert parse_publish_time(None) is None


def test_good_row_requires_region_for_pipeline():
    result = validate_and_clean_row(GOOD_ROW, None)
    assert result.valid is False
    assert result.reason == "UNKNOWN_REGION"


def test_good_row_clean_with_region():
    result = validate_and_clean_row(GOOD_ROW, "US")
    assert result.valid is True
    assert result.row["title"] == "My Video"
    assert result.row["views"] == 1000
    assert result.row["trending_date"] == "2017-11-14"
    assert result.row["engagement_ratio"] == 0.11
    assert result.row["dislikes"] == 0


def test_optional_fields_are_lenient():
    row = dict(GOOD_ROW, dislikes="not-a-number", comments_disabled="garbage")
    result = validate_and_clean_row(row, "US")
    assert result.valid is True
    assert result.row["dislikes"] == 0
    assert result.row["comments_disabled"] is False


def test_publish_time_is_threaded():
    row = dict(GOOD_ROW, publish_time="2017-11-10T17:00:03.000Z")
    result = validate_and_clean_row(row, "US")
    assert result.row["publish_time"] == "2017-11-10T17:00:03+00:00"


def test_boolean_flags_are_threaded():
    row = dict(GOOD_ROW, comments_disabled="true", ratings_disabled="false", video_error_or_removed="1")
    result = validate_and_clean_row(row, "US")
    assert result.row["comments_disabled"] is True
    assert result.row["ratings_disabled"] is False
    assert result.row["video_error_or_removed"] is True


def test_missing_required_field():
    row = dict(GOOD_ROW); del row["title"]
    result = validate_and_clean_row(row, "US")
    assert result.reason == "MISSING_REQUIRED_FIELD"


def test_empty_required_field():
    row = dict(GOOD_ROW, title="")
    result = validate_and_clean_row(row, "US")
    assert result.reason == "MISSING_REQUIRED_FIELD"


def test_bad_numeric_field():
    result = validate_and_clean_row(dict(GOOD_ROW, views="N/A"), "US")
    assert result.reason == "INVALID_NUMERIC_FIELD"


def test_fractional_numeric_field_rejected():
    result = validate_and_clean_row(dict(GOOD_ROW, likes="4.2"), "US")
    assert result.reason == "INVALID_NUMERIC_FIELD"


def test_negative_metric():
    result = validate_and_clean_row(dict(GOOD_ROW, likes="-5"), "US")
    assert result.reason == "NEGATIVE_METRIC"


def test_bad_date():
    result = validate_and_clean_row(dict(GOOD_ROW, trending_date="banana"), "US")
    assert result.reason == "BAD_DATE"


def test_zero_views_ratio_safe():
    result = validate_and_clean_row(dict(GOOD_ROW, views="0", likes="0"), "US")
    assert result.valid is True
    assert result.row["engagement_ratio"] == 0.0


def test_unknown_region_rejected():
    result = validate_and_clean_row(GOOD_ROW, None)
    assert result.reason == "UNKNOWN_REGION"


def test_dedupe_keeps_first():
    rows = [
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US", "views":100},
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US", "views":999},
    ]
    out = dedupe_silver_rows(rows)
    assert len(out) == 1 and out[0]["views"] == 100


def test_dedupe_separates_dates():
    rows = [
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US"},
        {"video_id":"a", "trending_date":"2017-11-15", "region":"US"},
    ]
    assert len(dedupe_silver_rows(rows)) == 2


def test_dedupe_separates_regions():
    rows = [
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US"},
        {"video_id":"a", "trending_date":"2017-11-14", "region":"GB"},
    ]
    assert len(dedupe_silver_rows(rows)) == 2


def test_dedupe_rejections_contains_reason():
    rows = [
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US"},
        {"video_id":"a", "trending_date":"2017-11-14", "region":"US"},
    ]
    accepted, rejected = dedupe_silver_rows_with_rejections(rows)
    assert len(accepted) == 1
    assert rejected[0]["quarantine_reason"] == DUPLICATE_ROW


def test_build_summary_grouping():
    rows = [
        {"category_id":24,"trending_date":"2017-11-14","region":"US","views":100,"likes":10,"dislikes":2,"comment_count":5,"engagement_ratio":0.15},
        {"category_id":24,"trending_date":"2017-11-14","region":"US","views":300,"likes":30,"dislikes":4,"comment_count":10,"engagement_ratio":0.133333},
    ]
    out = build_category_daily_summary(rows, {24:"Entertainment"})
    assert out[0]["video_count"] == 2
    assert out[0]["total_views"] == 400
    assert out[0]["category_name"] == "Entertainment"


def test_build_summary_fallback_category():
    rows = [{"category_id":99,"trending_date":"2017-11-14","region":"US","views":10,"likes":1,"dislikes":0,"comment_count":1,"engagement_ratio":0.2}]
    out = build_category_daily_summary(rows, {})
    assert out[0]["category_name"] == "Unknown (99)"


def test_build_summary_separates_regions():
    rows = [
        {"category_id":24,"trending_date":"2017-11-14","region":"US","views":100,"likes":10,"dislikes":0,"comment_count":5,"engagement_ratio":0.15},
        {"category_id":24,"trending_date":"2017-11-14","region":"GB","views":200,"likes":20,"dislikes":0,"comment_count":5,"engagement_ratio":0.125},
    ]
    out = build_category_daily_summary(rows, {24:"Entertainment"})
    assert len(out) == 2

def test_detect_schema_drift_no_drift():
    from transform_logic import detect_schema_drift, REQUIRED_FIELDS
    report = detect_schema_drift(REQUIRED_FIELDS)
    assert report["missing_required_columns"] == []

def test_detect_schema_drift_flags_new_column():
    from transform_logic import detect_schema_drift
    report = detect_schema_drift(["video_id", "brand_new_field_youtube_added"])
    assert "brand_new_field_youtube_added" in report["new_columns"]

def test_detect_schema_drift_flags_missing_required():
    from transform_logic import detect_schema_drift
    report = detect_schema_drift(["video_id"])  # missing most required fields
    assert report["drift_detected"] is True
    assert len(report["missing_required_columns"]) > 0

def test_dq_report_passes_threshold():
    report = data_quality_report(100, 98, [], 0, 0.95)
    assert report["pass"] is True and report["pass_rate"] == 0.98


def test_dq_report_fails_threshold():
    report = data_quality_report(100, 94, [], 0, 0.95)
    assert report["pass"] is False


def test_dq_report_counts_duplicates_separately():
    report = data_quality_report(100, 90, [{"quarantine_reason":"BAD_DATE"}], 5, 0.8)
    assert report["duplicate_rows"] == 5
    assert report["quarantined_rows"] == 5


def test_dq_report_empty_dataset_fails():
    report = data_quality_report(0, 0, [], 0, 0.95)
    assert report["pass"] is False
