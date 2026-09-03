"""AWS Glue Silver -> Gold ETL using distributed Spark aggregation + Redshift JDBC."""
from __future__ import annotations

import json
import sys

import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_PATH",
        "GOLD_PATH",
        "CATEGORY_REF_PATH",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_TABLE",
        "REDSHIFT_STAGE_TABLE",
        "REDSHIFT_TMP_DIR",
        "RUN_ID",
        "WATERMARK_PATH",
        "INCREMENTAL_MODE",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")
incremental_mode = args.get("INCREMENTAL_MODE", "false").strip().lower() == "true"


def _bucket_and_key(s3_uri: str):
    raw = s3_uri.replace("s3://", "", 1)
    bucket, key = raw.split("/", 1)
    return bucket, key


def _read_watermark(uri: str) -> str | None:
    bucket, key = _bucket_and_key(uri)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))["last_trending_date"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return None
        print(f"WARNING: watermark read failed, treating as first run: {exc}")
        return None
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: watermark read failed, treating as first run: {exc}")
        return None


def _write_watermark(uri: str, value: str) -> None:
    bucket, key = _bucket_and_key(uri)
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps({"last_trending_date": value}).encode("utf-8"),
        ContentType="application/json",
    )


spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
silver = spark.read.parquet(args["SILVER_PATH"]).withColumn("trending_date", F.to_date("trending_date"))

watermark_date = None
if incremental_mode:
    watermark_date = _read_watermark(args["WATERMARK_PATH"])
    if watermark_date:
        print(f"INCREMENTAL_MODE: processing rows with trending_date > {watermark_date}")
        silver = silver.filter(F.col("trending_date") > F.lit(watermark_date))
    else:
        print("INCREMENTAL_MODE: no watermark found — this is the first incremental run, processing full history once")

if incremental_mode and silver.rdd.isEmpty():
    print("INCREMENTAL_MODE: no new rows since last watermark — nothing to do")
    job.commit()
    sys.exit(0)

# Category names are enriched from the small JSON reference refreshed by the
# Bronze->Silver job. category_name is a lookup keyed only on category_id, so
# it's joined AFTER aggregation, not before — joining beforehand meant
# groupBy/agg silently dropped it, since it was neither a grouping key nor an
# aggregated column.
try:
    category_ref_path = args["CATEGORY_REF_PATH"].rstrip("/")
    category_ref = spark.read.option("multiLine", True).json(category_ref_path + "/youtube_categories.json")
    category_ref = category_ref.select(F.col("category_id").cast("int"), F.col("category_title").alias("category_name")).dropDuplicates(["category_id"])
except Exception as exc:
    print(f"WARNING: category reference unavailable: {exc}")
    category_ref = spark.createDataFrame([], "category_id INT, category_name STRING")

# Gold aggregation remains fully distributed and deterministic.
# Unknown IDs remain visible instead of being silently dropped.
gold = (
    silver.groupBy("category_id", "trending_date", "region")
    .agg(
        F.count(F.lit(1)).cast("long").alias("video_count"),
        F.sum("views").cast("long").alias("total_views"),
        F.sum("likes").cast("long").alias("total_likes"),
        F.sum(F.coalesce(F.col("dislikes"), F.lit(0))).cast("long").alias("total_dislikes"),
        F.sum("comment_count").cast("long").alias("total_comments"),
        F.round(F.avg("views"), 2).alias("avg_views_per_video"),
        F.round(F.avg("engagement_ratio"), 6).alias("avg_engagement_ratio"),
    )
    .join(F.broadcast(category_ref), on="category_id", how="left")
    .withColumn("category_name", F.coalesce(F.col("category_name"), F.concat(F.lit("Unknown ("), F.col("category_id").cast("string"), F.lit(")"))))
    .select(
        "category_id", "category_name", "trending_date", "region",
        "video_count", "total_views", "total_likes", "total_dislikes",
        "total_comments", "avg_views_per_video", "avg_engagement_ratio",
    )
)

gold = gold.persist()
gold_row_count = gold.count()
print(f"Gold rows this run: {gold_row_count} (incremental_mode={incremental_mode})")

gold.write.mode("overwrite" if not incremental_mode else "append") \
    .partitionBy("region", "trending_date").parquet(args["GOLD_PATH"])

gold_dyf = DynamicFrame.fromDF(gold, glue_context, "gold_dyf")

if incremental_mode:
    postactions = f"""
        MERGE INTO {args['REDSHIFT_TABLE']}
        USING {args['REDSHIFT_STAGE_TABLE']} AS stage
        ON {args['REDSHIFT_TABLE']}.category_id = stage.category_id
           AND {args['REDSHIFT_TABLE']}.trending_date = stage.trending_date
           AND {args['REDSHIFT_TABLE']}.region = stage.region
        WHEN MATCHED THEN UPDATE SET
            category_name = stage.category_name,
            video_count = stage.video_count,
            total_views = stage.total_views,
            total_likes = stage.total_likes,
            total_dislikes = stage.total_dislikes,
            total_comments = stage.total_comments,
            avg_views_per_video = stage.avg_views_per_video,
            avg_engagement_ratio = stage.avg_engagement_ratio
        WHEN NOT MATCHED THEN INSERT (
            category_id, category_name, trending_date, region, video_count,
            total_views, total_likes, total_dislikes, total_comments,
            avg_views_per_video, avg_engagement_ratio
        ) VALUES (
            stage.category_id, stage.category_name, stage.trending_date, stage.region,
            stage.video_count, stage.total_views, stage.total_likes, stage.total_dislikes,
            stage.total_comments, stage.avg_views_per_video, stage.avg_engagement_ratio
        );
        TRUNCATE TABLE {args['REDSHIFT_STAGE_TABLE']};
    """
else:
    postactions = f"""
        TRUNCATE TABLE {args['REDSHIFT_TABLE']};
        INSERT INTO {args['REDSHIFT_TABLE']}
        SELECT category_id, category_name, trending_date, region,
               video_count, total_views, total_likes, total_dislikes,
               total_comments, avg_views_per_video, avg_engagement_ratio
        FROM {args['REDSHIFT_STAGE_TABLE']};
        TRUNCATE TABLE {args['REDSHIFT_STAGE_TABLE']};
    """

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=gold_dyf,
    catalog_connection=args["REDSHIFT_CONNECTION"],
    connection_options={
        "dbtable": args["REDSHIFT_STAGE_TABLE"],
        "database": "youtube_lakehouse",
        "preactions": f"""
            CREATE SCHEMA IF NOT EXISTS gold;
            CREATE TABLE IF NOT EXISTS {args['REDSHIFT_STAGE_TABLE']} (
                category_id INTEGER,
                category_name VARCHAR(256),
                trending_date DATE,
                region VARCHAR(8),
                video_count BIGINT,
                total_views BIGINT,
                total_likes BIGINT,
                total_dislikes BIGINT,
                total_comments BIGINT,
                avg_views_per_video DOUBLE PRECISION,
                avg_engagement_ratio DOUBLE PRECISION
            );
            TRUNCATE TABLE {args['REDSHIFT_STAGE_TABLE']};
        """,
        "postactions": postactions,
        "redshiftTmpDir": args["REDSHIFT_TMP_DIR"],
    },
    redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
)

if incremental_mode and gold_row_count > 0:
    new_watermark = gold.agg(F.max("trending_date")).collect()[0][0]
    if new_watermark:
        _write_watermark(args["WATERMARK_PATH"], str(new_watermark))
        print(f"INCREMENTAL_MODE: watermark advanced to {new_watermark}")

job.commit()