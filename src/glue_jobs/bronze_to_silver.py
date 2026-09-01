"""AWS Glue Bronze -> Silver ETL.

The transformation/validation functions live in transform_logic.py so they can
be unit-tested without Glue. This job keeps Spark distributed: validation is
performed partition-by-partition and only small DQ counters are collected to
Python. The full Bronze dataset is never collected onto the driver.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from api_client import fetch_category_metadata
from category_enrichment import parse_category_api_response
from transform_logic import data_quality_report, extract_region_from_filename, validate_and_clean_row


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_PATH",
        "SILVER_PATH",
        "QUARANTINE_PATH",
        "DQ_REPORT_PATH",
        "CATEGORY_REF_PATH",
        "YOUTUBE_API_KEY_SECRET_NAME",
        "MIN_DQ_PASS_RATE",
        "BRONZE_FILE",
        "DQ_REPORT_KEY",
        "RUN_ID",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


def _bucket_and_key(s3_uri: str):
    raw = s3_uri.replace("s3://", "", 1)
    bucket, key = raw.split("/", 1)
    return bucket, key


def _write_json_s3(uri: str, payload: dict) -> None:
    bucket, prefix = _bucket_and_key(uri.rstrip("/"))
    key = f"{prefix}/{args['RUN_ID']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"), ContentType="application/json")


def _read_api_key(secret_name: str) -> str:
    value = secrets.get_secret_value(SecretId=secret_name)
    return value["SecretString"].strip()


def _refresh_category_reference() -> dict[int, str]:
    try:
        response = fetch_category_metadata(_read_api_key(args["YOUTUBE_API_KEY_SECRET_NAME"]))
        rows = parse_category_api_response(response)
        lookup = {int(row["category_id"]): row["category_title"] for row in rows}
        if rows:
            bucket, prefix = _bucket_and_key(args["CATEGORY_REF_PATH"].rstrip("/"))
            s3.put_object(
                Bucket=bucket,
                Key=f"{prefix}/youtube_categories.json",
                Body=json.dumps(rows).encode("utf-8"),
                ContentType="application/json",
            )
        return lookup
    except Exception as exc:  # noqa: BLE001 - API fallback is intentional.
        print(f"WARNING: YouTube category API refresh failed: {exc}")
        return {}


def _load_existing_category_reference() -> dict[int, str]:
    bucket, prefix = _bucket_and_key(args["CATEGORY_REF_PATH"].rstrip("/"))
    key = f"{prefix}/youtube_categories.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        rows = json.loads(obj["Body"].read().decode("utf-8"))
        return {int(row["category_id"]): row["category_title"] for row in rows}
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return {}
        print(f"WARNING: category fallback read failed: {exc}")
        return {}
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: category fallback read failed: {exc}")
        return {}


def _clean_value(row_dict: dict, source_file: str):
    region = extract_region_from_filename(source_file)
    result = validate_and_clean_row(row_dict, region)
    if result.valid:
        result.row["source_file"] = source_file
        return "clean", json.dumps(result.row, default=str)
    payload = dict(result.row)
    payload["region"] = region
    payload["quarantine_reason"] = result.reason
    payload["source_file"] = source_file
    return "reject", json.dumps(payload, default=str)


bronze_target = args["BRONZE_PATH"].rstrip("/")
bronze_file = args["BRONZE_FILE"].strip()
input_path = f"{bronze_target}/{bronze_file}" if bronze_file else bronze_target

print(f"Reading Bronze: {input_path}")
bronze_df = (
    spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(input_path)
    .withColumn("_source_file", F.input_file_name())
)
bronze_df = bronze_df.persist(StorageLevel.MEMORY_AND_DISK)
total_rows = bronze_df.count()
if total_rows == 0:
    raise RuntimeError(f"No Bronze rows found at {input_path}")

validated = bronze_df.rdd.mapPartitions(
    lambda iterator: (_clean_value(row.asDict(recursive=True), row["_source_file"]) for row in iterator)
).persist(StorageLevel.MEMORY_AND_DISK)

clean_count = validated.filter(lambda x: x[0] == "clean").count()
rejected = validated.filter(lambda x: x[0] == "reject").map(lambda x: x[1])
rejected_count = rejected.count()

# If nothing survives validation, still emit a complete DQ report and
# quarantine output. Do not ask Spark to infer a schema from an empty RDD.
if clean_count == 0:
    reason_counts = dict(validated.filter(lambda x: x[0] == "reject").map(
        lambda x: json.loads(x[1].decode() if isinstance(x[1], bytes) else x[1]).get("quarantine_reason")
    ).countByValue())
    reject_df = spark.read.json(rejected) if rejected_count else None
    if reject_df is not None:
        reject_df.write.mode("append").partitionBy("quarantine_reason").parquet(args["QUARANTINE_PATH"])
    report = data_quality_report(
        total_rows=total_rows, clean_rows=0, rejected_rows=[],
        duplicate_rows=0, threshold=float(args["MIN_DQ_PASS_RATE"]),
    )
    report["reasons"] = reason_counts
    report["rejected_validation_rows"] = int(rejected_count)
    report["quarantined_total_rows"] = int(rejected_count)
    report["run_id"] = args["RUN_ID"]
    report["input_path"] = input_path
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report_bucket, report_prefix = _bucket_and_key(args["DQ_REPORT_PATH"].rstrip("/"))
    report_key = args["DQ_REPORT_KEY"].strip() or f"{report_prefix}/{args['RUN_ID']}.json"
    s3.put_object(Bucket=report_bucket, Key=report_key, Body=json.dumps(report).encode("utf-8"), ContentType="application/json")
    print(json.dumps({"DQ_REPORT": report}, indent=2))
    bronze_df.unpersist()
    validated.unpersist()
    # NOTE: no sys.exit(0) here — just fall through to job.commit() at the bottom of the script
else:
    clean_df = spark.read.json(validated.filter(lambda x: x[0] == "clean").map(lambda x: x[1]))
    clean_df = clean_df.persist(StorageLevel.MEMORY_AND_DISK)
    lookup = _refresh_category_reference()
    if not lookup:
        lookup = _load_existing_category_reference()
    print(f"Category reference entries available: {len(lookup)}")
    category_rows = list(lookup.items()) or [(-1, "Unknown (-1)")]
    category_ref_df = spark.createDataFrame(category_rows, "category_id INT, category_name STRING")
    from pyspark.sql.window import Window
    window = Window.partitionBy("video_id", "trending_date", "region").orderBy(F.col("source_file"))
    ranked = clean_df.withColumn("_rn", F.row_number().over(window))
    duplicate_df = ranked.filter(F.col("_rn") > 1).drop("_rn")
    silver_df = ranked.filter(F.col("_rn") == 1).drop("_rn")
    silver_df = silver_df.join(F.broadcast(category_ref_df), on="category_id", how="left") \
        .withColumn("category_name", F.coalesce(F.col("category_name"), F.concat(F.lit("Unknown ("), F.col("category_id").cast("string"), F.lit(")"))))
    duplicate_count = duplicate_df.count()
    silver_df.write.mode("overwrite").partitionBy("region", "trending_date").parquet(args["SILVER_PATH"])
    reject_df = spark.read.json(rejected) if rejected_count else None
    if reject_df is not None:
        reject_df.write.mode("append").partitionBy("quarantine_reason").parquet(args["QUARANTINE_PATH"])
    if duplicate_count:
        duplicate_df.withColumn("quarantine_reason", F.lit("DUPLICATE_ROW")) \
            .write.mode("append").partitionBy("quarantine_reason").parquet(args["QUARANTINE_PATH"])
    quarantine_count = int(rejected_count) + int(duplicate_count)
    clean_after_dedupe = int(silver_df.count())
    threshold = float(args["MIN_DQ_PASS_RATE"])
    reason_counts = dict(validated.filter(lambda x: x[0] == "reject").map(
        lambda x: json.loads(x[1].decode() if isinstance(x[1], bytes) else x[1]).get("quarantine_reason")
    ).countByValue())
    report = data_quality_report(
        total_rows=total_rows, clean_rows=clean_after_dedupe, rejected_rows=[],
        duplicate_rows=duplicate_count, threshold=threshold,
    )
    reason_counts["DUPLICATE_ROW"] = int(duplicate_count) if duplicate_count else 0
    report["reasons"] = reason_counts
    report["rejected_validation_rows"] = int(rejected_count)
    report["quarantined_total_rows"] = quarantine_count
    report["run_id"] = args["RUN_ID"]
    report["input_path"] = input_path
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report_bucket, report_prefix = _bucket_and_key(args["DQ_REPORT_PATH"].rstrip("/"))
    report_key = args["DQ_REPORT_KEY"].strip() or f"{report_prefix}/{args['RUN_ID']}.json"
    s3.put_object(Bucket=report_bucket, Key=report_key, Body=json.dumps(report).encode("utf-8"), ContentType="application/json")
    print(json.dumps({"DQ_REPORT": report}, indent=2))
    bronze_df.unpersist()
    validated.unpersist()
    clean_df.unpersist()

job.commit()
