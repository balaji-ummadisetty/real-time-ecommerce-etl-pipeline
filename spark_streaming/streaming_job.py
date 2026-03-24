"""
streaming_job.py
----------------
PySpark Structured Streaming job.

Reads from two Kafka topics, applies transformations, and writes to S3
in partitioned Parquet format (raw + processed layers).

Run with:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.261 \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
      spark_streaming/streaming_job.py
"""

from __future__ import annotations

import logging
import os
import sys
import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Allow imports from this file's directory (spark_streaming/)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema_definitions import user_activity_schema, purchase_schema
from transformations import (
    enrich_activity, enrich_purchase, explode_order_items,
    compute_sales_agg, compute_funnel_agg,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml"
)

with open(_CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

KAFKA_BROKERS    = cfg["kafka"]["bootstrap_servers"]
S3_BUCKET        = cfg["aws"]["s3_bucket"]
CHECKPOINT_BASE  = cfg["spark"]["checkpoint_base"]
TRIGGER_INTERVAL = cfg["spark"]["trigger_interval"]     # e.g. "30 seconds"


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceStreamingPipeline")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.streaming.schemaInference", "false")
        # S3A performance tuning
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728")       # 128 MB
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Kafka reader factory
# ---------------------------------------------------------------------------

def kafka_stream(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)   # back-pressure
        .load()
    )


# ---------------------------------------------------------------------------
# Parse Kafka value bytes -> typed DataFrame
# ---------------------------------------------------------------------------

def parse_kafka(raw_df, schema):
    return (
        raw_df
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        )
        .select("kafka_partition", "kafka_offset", "data.*")
    )


# ---------------------------------------------------------------------------
# S3 sink writers
# ---------------------------------------------------------------------------

def write_to_s3(df, path: str, checkpoint: str, partition_cols: list, trigger: str):
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy(*partition_cols)
        .trigger(processingTime=trigger)
        .start()
    )


def write_agg_to_s3(df, path: str, checkpoint: str, trigger: str):
    return (
        df.writeStream
        .format("parquet")
        .outputMode("complete")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .start()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")

    # -- 1. Read raw Kafka streams
    raw_activity  = kafka_stream(spark, "user_activity")
    raw_purchases = kafka_stream(spark, "purchases")

    # -- 2. Parse JSON
    activity_df = parse_kafka(raw_activity,  user_activity_schema)
    purchase_df = parse_kafka(raw_purchases, purchase_schema)

    # -- 3. Write RAW layer to S3 (schema-on-read)
    q_raw_activity = write_to_s3(
        df=activity_df,
        path=f"s3a://{S3_BUCKET}/raw/user_activity/",
        checkpoint=f"{CHECKPOINT_BASE}/raw_activity",
        partition_cols=["year", "month", "day", "hour"],
        trigger=TRIGGER_INTERVAL,
    )

    q_raw_purchases = write_to_s3(
        df=purchase_df,
        path=f"s3a://{S3_BUCKET}/raw/purchases/",
        checkpoint=f"{CHECKPOINT_BASE}/raw_purchases",
        partition_cols=["year", "month", "day", "hour"],
        trigger=TRIGGER_INTERVAL,
    )

    # -- 4. Enrich & write PROCESSED layer
    enriched_activity = enrich_activity(activity_df)
    enriched_purchase = enrich_purchase(purchase_df)

    q_proc_activity = write_to_s3(
        df=enriched_activity,
        path=f"s3a://{S3_BUCKET}/processed/user_activity/",
        checkpoint=f"{CHECKPOINT_BASE}/proc_activity",
        partition_cols=["year", "month", "day"],
        trigger=TRIGGER_INTERVAL,
    )

    q_proc_purchases = write_to_s3(
        df=enriched_purchase,
        path=f"s3a://{S3_BUCKET}/processed/purchases/",
        checkpoint=f"{CHECKPOINT_BASE}/proc_purchases",
        partition_cols=["year", "month", "day"],
        trigger=TRIGGER_INTERVAL,
    )

    # -- 5. Item-level explode layer
    items_df = explode_order_items(enriched_purchase)

    q_items = write_to_s3(
        df=items_df,
        path=f"s3a://{S3_BUCKET}/processed/order_items/",
        checkpoint=f"{CHECKPOINT_BASE}/order_items",
        partition_cols=["year", "month", "day", "category"],
        trigger=TRIGGER_INTERVAL,
    )

    # -- 6. Aggregated layer (windowed)
    purchase_with_watermark = enriched_purchase.withWatermark("event_ts", "5 minutes")
    activity_with_watermark = enriched_activity.withWatermark("event_ts", "5 minutes")

    sales_agg  = compute_sales_agg(purchase_with_watermark)
    funnel_agg = compute_funnel_agg(activity_with_watermark)

    q_sales_agg = write_agg_to_s3(
        df=sales_agg,
        path=f"s3a://{S3_BUCKET}/aggregated/sales/",
        checkpoint=f"{CHECKPOINT_BASE}/sales_agg",
        trigger="5 minutes",
    )

    q_funnel_agg = write_agg_to_s3(
        df=funnel_agg,
        path=f"s3a://{S3_BUCKET}/aggregated/funnel/",
        checkpoint=f"{CHECKPOINT_BASE}/funnel_agg",
        trigger="10 minutes",
    )

    log.info("All streaming queries started:")
    for q in [q_raw_activity, q_raw_purchases, q_proc_activity,
              q_proc_purchases, q_items, q_sales_agg, q_funnel_agg]:
        log.info("  query=%s  status=%s", q.name or q.id, q.status)

    # Block until any query terminates
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
