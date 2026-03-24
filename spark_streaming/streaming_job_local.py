"""
streaming_job_local.py
----------------------
PySpark Structured Streaming job — LOCAL MODE (no AWS/S3 needed).
Reads from Kafka, applies transformations, writes Parquet to local folders.

Run with (replace Spark version to match yours):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark_streaming/streaming_job_local.py

Find your Spark version:
    spark-submit --version

Then match the package:
    Spark 3.3.x  →  spark-sql-kafka-0-10_2.12:3.3.0
    Spark 3.4.x  →  spark-sql-kafka-0-10_2.12:3.4.0
    Spark 3.5.x  →  spark-sql-kafka-0-10_2.12:3.5.0
    Spark 4.0.x  →  spark-sql-kafka-0-10_2.13:4.0.0
"""

import sys
import os
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_streaming.schema_definitions import user_activity_schema, purchase_schema
from spark_streaming.transformations import (
    enrich_activity, enrich_purchase, explode_order_items,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all local paths, no AWS
# ---------------------------------------------------------------------------

KAFKA_BROKERS    = "localhost:9092"
_PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_BASE      = os.path.join(_PROJECT_ROOT, "output")
CHECKPOINT_BASE  = os.path.join(_PROJECT_ROOT, "checkpoints")
TRIGGER_INTERVAL = "10 seconds"
# ---------------------------------------------------------------------------
# Spark session — local mode
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("EcommerceStreamingLocal")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")       # small for local
    .config("spark.ui.port", "4040")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
log.info("✅ Spark session started in local mode")

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------

def kafka_stream(topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

# ---------------------------------------------------------------------------
# Parse JSON from Kafka value bytes
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
        # Drop rows where JSON parsing failed (malformed messages)
        .filter(F.col("event_id").isNotNull())
    )

# ---------------------------------------------------------------------------
# Write helpers — local Parquet
# ---------------------------------------------------------------------------

def write_stream(df, name: str, partition_cols: list):
    path       = f"{OUTPUT_BASE}/{name}"
    checkpoint = f"{CHECKPOINT_BASE}/{name}"
    log.info(f"Starting stream: {name} → {path}")
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy(*partition_cols)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

def write_stream_console(df, name: str, num_rows: int = 5):
    """Also print a sample to console so you can see data flowing."""
    return (
        df.writeStream
        .format("console")
        .outputMode("append")
        .option("numRows", num_rows)
        .option("truncate", False)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(name)
        .start()
    )

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_BASE,     exist_ok=True)
    os.makedirs(CHECKPOINT_BASE, exist_ok=True)

    # ── 1. Read raw Kafka streams ──────────────────────────────────────────
    log.info("Reading from Kafka topics: user_activity, purchases")
    raw_activity  = kafka_stream("user_activity")
    raw_purchases = kafka_stream("purchases")

    print('Raw Kafka schema (user_activity):')
    raw_activity.printSchema()
    print('Raw Kafka schema (purchases):')
    raw_purchases.printSchema()
    print()


    # ── 2. Parse JSON ──────────────────────────────────────────────────────
    activity_df  = parse_kafka(raw_activity,  user_activity_schema)
    purchase_df  = parse_kafka(raw_purchases, purchase_schema)

    print('Parsed schema (user_activity):')
    activity_df.printSchema()
    print('Parsed schema (purchases):')
    purchase_df.printSchema()
    print()

    # ── 3. Enrich ──────────────────────────────────────────────────────────
    enriched_activity = enrich_activity(activity_df)
    enriched_purchase = enrich_purchase(purchase_df)
    items_df          = explode_order_items(enriched_purchase)

    # ── 4. Write processed Parquet locally ────────────────────────────────
    q1 = write_stream(enriched_activity, "processed/user_activity", ["year","month","day"])
    q2 = write_stream(enriched_purchase, "processed/purchases",     ["year","month","day"])
    q3 = write_stream(items_df,          "processed/order_items",   ["year","month","day"])

    # ── 5. Print sample to console so you can see it working ──────────────
    q4 = write_stream_console(enriched_purchase, "console_purchases", num_rows=3)

    log.info("🎯 All streaming queries running!")
    log.info(f"   • Output  → {os.path.abspath(OUTPUT_BASE)}")
    log.info(f"   • Spark UI → http://localhost:4040")
    log.info("   Press Ctrl+C to stop.\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()