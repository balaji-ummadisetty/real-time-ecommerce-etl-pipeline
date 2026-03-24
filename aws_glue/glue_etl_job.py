"""
glue_etl_job.py
---------------
AWS Glue ETL script that runs on a scheduled basis to:
  1. Read processed Parquet data from S3
  2. Apply business-level transformations and data quality checks
  3. Write curated data back to S3 in optimized format
  4. Load summary tables into Amazon Redshift

Deploy this script to S3 and reference it from your Glue Job definition.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import boto3
import logging

# ---------------------------------------------------------------------------
# Job init
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET",
    "REDSHIFT_CONNECTION",
    "REDSHIFT_DB",
    "PROCESSING_DATE",          # YYYY-MM-DD — injected by EventBridge schedule
])

sc      = SparkContext()
glueCtx = GlueContext(sc)
spark   = glueCtx.spark_session
job     = Job(glueCtx)
job.init(args["JOB_NAME"], args)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

S3_BUCKET       = args["S3_BUCKET"]
REDSHIFT_CONN   = args["REDSHIFT_CONNECTION"]
REDSHIFT_DB     = args["REDSHIFT_DB"]
PROC_DATE       = args["PROCESSING_DATE"]   # e.g. "2024-03-15"

PROCESSED_PATH  = f"s3://{S3_BUCKET}/processed"
CURATED_PATH    = f"s3://{S3_BUCKET}/curated"

# ---------------------------------------------------------------------------
# Helper: read a Glue catalog table partition for a specific date
# ---------------------------------------------------------------------------

def read_catalog(database: str, table: str, pushdown_predicate: str = None) -> DynamicFrame:
    opts = {
        "database":         database,
        "table_name":       table,
        "transformation_ctx": f"read_{table}",
    }
    if pushdown_predicate:
        opts["push_down_predicate"] = pushdown_predicate

    return glueCtx.create_dynamic_frame.from_catalog(**opts)


# ---------------------------------------------------------------------------
# 1. Read processed data for PROC_DATE
# ---------------------------------------------------------------------------

year, month, day = PROC_DATE.split("-")
predicate = f"year == {year} and month == {int(month)} and day == {int(day)}"

log.info(f"Reading processed data for date: {PROC_DATE}")

purchases_dyf = read_catalog("ecommerce_processed", "purchases", predicate)
activity_dyf  = read_catalog("ecommerce_processed", "user_activity", predicate)
items_dyf     = read_catalog("ecommerce_processed", "order_items", predicate)

purchases_df = purchases_dyf.toDF()
activity_df  = activity_dyf.toDF()
items_df     = items_dyf.toDF()

log.info(f"Records loaded – purchases={purchases_df.count():,}  activity={activity_df.count():,}")


# ---------------------------------------------------------------------------
# 2. Data Quality checks
# ---------------------------------------------------------------------------

def dq_check(df, name: str):
    total = df.count()
    nulls = df.filter(F.col("event_id").isNull()).count()
    dupes = total - df.dropDuplicates(["event_id"]).count()
    log.info(f"DQ [{name}] total={total:,}  null_event_id={nulls}  duplicates={dupes}")
    # Drop duplicates before proceeding
    return df.dropDuplicates(["event_id"])

purchases_df = dq_check(purchases_df, "purchases")
activity_df  = dq_check(activity_df,  "user_activity")
items_df     = dq_check(items_df,     "order_items")


# ---------------------------------------------------------------------------
# 3. Curated: Daily sales summary
# ---------------------------------------------------------------------------

daily_sales = (
    purchases_df
    .filter(F.col("payment_status") == "success")
    .groupBy("date", "country_code", "payment_currency")
    .agg(
        F.count("order_id").alias("orders"),
        F.sum("order_total").alias("gross_revenue"),
        F.sum("discount_amount").alias("total_discounts"),
        F.sum("order_total").alias("net_revenue"),
        F.avg("order_total").alias("avg_order_value"),
        F.countDistinct("user_id").alias("unique_buyers"),
        F.sum("item_count").alias("units_sold"),
        F.sum(F.when(F.col("is_gift"), 1).otherwise(0)).alias("gift_orders"),
    )
    .withColumn("processing_date", F.lit(PROC_DATE))
)

# ---------------------------------------------------------------------------
# 4. Curated: Category performance
# ---------------------------------------------------------------------------

category_perf = (
    items_df
    .filter(F.col("payment_status") == "success")
    .groupBy("date", "category", "subcategory")
    .agg(
        F.sum("item_revenue").alias("revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("orders_with_category"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.countDistinct("user_id").alias("unique_buyers"),
    )
    .withColumn("processing_date", F.lit(PROC_DATE))
)

# ---------------------------------------------------------------------------
# 5. Curated: User funnel metrics
# ---------------------------------------------------------------------------

funnel_events = ["page_view", "product_view", "add_to_cart", "checkout_start"]

funnel = (
    activity_df
    .filter(F.col("event_type").isin(funnel_events))
    .groupBy("date", "event_type", "country_code", "device_type", "channel")
    .agg(
        F.count("event_id").alias("event_count"),
        F.countDistinct("session_id").alias("sessions"),
        F.countDistinct("user_id").alias("users"),
        F.avg("session_duration_sec").alias("avg_session_sec"),
        F.sum(F.when(F.col("is_bounce"), 1).otherwise(0)).alias("bounces"),
    )
    .withColumn("bounce_rate", F.col("bounces") / F.col("sessions"))
    .withColumn("processing_date", F.lit(PROC_DATE))
)

# ---------------------------------------------------------------------------
# 6. Write curated data to S3
# ---------------------------------------------------------------------------

def write_curated(df, name: str):
    path = f"{CURATED_PATH}/{name}/"
    log.info(f"Writing curated/{name} → {path}")
    dyf = DynamicFrame.fromDF(df, glueCtx, name)
    glueCtx.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={"path": path, "partitionKeys": ["date"]},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx=f"write_{name}",
    )

write_curated(daily_sales,   "daily_sales")
write_curated(category_perf, "category_performance")
write_curated(funnel,        "funnel_metrics")

# ---------------------------------------------------------------------------
# 7. Load into Redshift (COPY via S3)
# ---------------------------------------------------------------------------

REDSHIFT_TABLES = {
    "daily_sales":            daily_sales,
    "category_performance":   category_perf,
    "funnel_metrics":         funnel,
}

for table_name, df in REDSHIFT_TABLES.items():
    log.info(f"Loading {table_name} → Redshift")
    dyf = DynamicFrame.fromDF(df, glueCtx, table_name)
    glueCtx.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=REDSHIFT_CONN,
        connection_options={
            "database":   REDSHIFT_DB,
            "dbtable":    f"ecommerce.{table_name}",
            "preactions": f"DELETE FROM ecommerce.{table_name} WHERE date = '{PROC_DATE}'",
        },
        redshift_tmp_dir=f"s3://{S3_BUCKET}/tmp/redshift/",
        transformation_ctx=f"rs_{table_name}",
    )

# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------

job.commit()
log.info("✅ Glue ETL job completed successfully.")
