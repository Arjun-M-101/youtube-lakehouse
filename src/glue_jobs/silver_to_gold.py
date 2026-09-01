"""AWS Glue Silver -> Gold ETL using distributed Spark aggregation + Redshift JDBC."""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame   # ADD THIS LINE
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
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
silver = spark.read.parquet(args["SILVER_PATH"]).withColumn("trending_date", F.to_date("trending_date"))

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
        "category_id",
        "category_name",
        "trending_date",
        "region",
        "video_count",
        "total_views",
        "total_likes",
        "total_dislikes",
        "total_comments",
        "avg_views_per_video",
        "avg_engagement_ratio",
    )
)

# Update category names from the Glue Catalog if a reference Parquet/JSON table
# is later introduced. For this project the stable business key is category_id,
# and unknown labels are explicit rather than fatal.

gold.write.mode("overwrite").partitionBy("region", "trending_date").parquet(args["GOLD_PATH"])

gold_dyf = DynamicFrame.fromDF(gold, glue_context, "gold_dyf")

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
        "postactions": f"""
            TRUNCATE TABLE {args['REDSHIFT_TABLE']};
            INSERT INTO {args['REDSHIFT_TABLE']}
            SELECT category_id, category_name, trending_date, region,
                   video_count, total_views, total_likes, total_dislikes,
                   total_comments, avg_views_per_video, avg_engagement_ratio
            FROM {args['REDSHIFT_STAGE_TABLE']};
            TRUNCATE TABLE {args['REDSHIFT_STAGE_TABLE']};
        """,
        "redshiftTmpDir": args["REDSHIFT_TMP_DIR"],
    },
    redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
)

job.commit()