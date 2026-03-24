"""
transformations.py
------------------
Pure transformation functions applied to raw Kafka DataFrames.
Each function takes a streaming DataFrame and returns a new one.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------------------------
# User activity transformations
# ---------------------------------------------------------------------------

def enrich_activity(df: DataFrame) -> DataFrame:
    """
    Enrich raw user activity events:
      • Parse ISO timestamp → proper TimestampType
      • Add partition columns (year/month/day/hour)
      • Classify referrer into channel buckets
      • Flag high-value sessions
    """
    return (
        df
        # Parse timestamp
        .withColumn("event_ts",   F.to_timestamp("timestamp"))
        .withColumn("year",       F.year("event_ts"))
        .withColumn("month",      F.month("event_ts"))
        .withColumn("day",        F.dayofmonth("event_ts"))
        .withColumn("hour",       F.hour("event_ts"))
        .withColumn("date",       F.to_date("event_ts"))

        # Classify traffic channel
        .withColumn("channel",
            F.when(F.col("referrer").contains("google"), "organic_search")
             .when(F.col("referrer").contains("bing"),   "organic_search")
             .when(F.col("referrer").contains("facebook"), "social")
             .when(F.col("referrer").contains("instagram"), "social")
             .when(F.col("referrer").contains("youtube"), "social")
             .when(F.col("referrer") == "email_campaign", "email")
             .when(F.col("referrer").isNull(), "direct")
             .otherwise("referral")
        )

        # High-value session flag
        .withColumn("is_high_value_session",
            (F.col("session_duration_sec") > 300) &
            (F.col("page_views_in_session") > 5) &
            (~F.col("is_bounce"))
        )

        # Flatten device and geo for easier querying
        .withColumn("device_type",    F.col("device.device_type"))
        .withColumn("os",             F.col("device.os"))
        .withColumn("browser",        F.col("device.browser"))
        .withColumn("country",        F.col("geo.country"))
        .withColumn("country_code",   F.col("geo.country_code"))
        .withColumn("city",           F.col("geo.city"))

        .drop("device", "geo", "timestamp")
    )


# ---------------------------------------------------------------------------
# Purchase transformations
# ---------------------------------------------------------------------------

def enrich_purchase(df: DataFrame) -> DataFrame:
    """
    Enrich purchase events:
      • Parse timestamp + partition columns
      • Compute derived revenue metrics
      • Explode items array for item-level analysis
      • Classify order size
    """
    enriched = (
        df
        .withColumn("event_ts",   F.to_timestamp("timestamp"))
        .withColumn("year",       F.year("event_ts"))
        .withColumn("month",      F.month("event_ts"))
        .withColumn("day",        F.dayofmonth("event_ts"))
        .withColumn("hour",       F.hour("event_ts"))
        .withColumn("date",       F.to_date("event_ts"))

        # Revenue metrics
        .withColumn("effective_discount_pct",
            F.when(F.col("subtotal") > 0,
                (F.col("discount_amount") / F.col("subtotal")).cast(DoubleType())
            ).otherwise(0.0)
        )

        # Order size bucket
        .withColumn("order_size",
            F.when(F.col("order_total") < 50,   "small")
             .when(F.col("order_total") < 200,  "medium")
             .when(F.col("order_total") < 500,  "large")
             .otherwise("whale")
        )

        # Flatten nested fields
        .withColumn("payment_method",   F.col("payment.method"))
        .withColumn("payment_status",   F.col("payment.status"))
        .withColumn("payment_currency", F.col("payment.currency"))
        .withColumn("transaction_id",   F.col("payment.transaction_id"))
        .withColumn("device_type",      F.col("device.device_type"))
        .withColumn("country",          F.col("geo.country"))
        .withColumn("country_code",     F.col("geo.country_code"))
        .withColumn("city",             F.col("geo.city"))

        .drop("payment", "device", "geo", "timestamp")
    )
    return enriched


def explode_order_items(df: DataFrame) -> DataFrame:
    """
    Creates item-level DataFrame by exploding the items array.
    Used for category-level revenue analysis.
    """
    return (
        df
        .withColumn("item",          F.explode("items"))
        .withColumn("product_id",    F.col("item.product_id"))
        .withColumn("product_name",  F.col("item.product_name"))
        .withColumn("category",      F.col("item.category"))
        .withColumn("subcategory",   F.col("item.subcategory"))
        .withColumn("brand",         F.col("item.brand"))
        .withColumn("sku",           F.col("item.sku"))
        .withColumn("unit_price",    F.col("item.price"))
        .withColumn("quantity",      F.col("item.quantity"))
        .withColumn("discount_pct",  F.col("item.discount_pct"))
        .withColumn("item_revenue",  F.col("item.final_price"))
        .drop("item", "items")
    )


# ---------------------------------------------------------------------------
# Aggregate transformations (micro-batch window aggregations)
# ---------------------------------------------------------------------------

def compute_sales_agg(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """
    Tumbling window aggregation: sales metrics per 5-minute window by country.
    """
    return (
        df
        .filter(F.col("payment_status") == "success")
        .groupBy(
            F.window("event_ts", window_duration),
            "country_code",
            "payment_currency",
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.sum("item_count").alias("items_sold"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


def compute_funnel_agg(df: DataFrame, window_duration: str = "10 minutes") -> DataFrame:
    """
    Conversion funnel aggregation per 10-minute window.
    """
    return (
        df
        .groupBy(
            F.window("event_ts", window_duration),
            "event_type",
            "country_code",
            "device_type",
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("session_duration_sec").alias("avg_session_sec"),
            F.sum(F.when(F.col("is_bounce"), 1).otherwise(0)).alias("bounce_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )
