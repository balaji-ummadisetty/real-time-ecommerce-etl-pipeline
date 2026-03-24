"""
schema_definitions.py
---------------------
PySpark StructType schemas derived from event_schemas.py Pydantic models.
Used by streaming_job.py to parse JSON from Kafka topics.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType, DoubleType, ArrayType,
)

# ---------------------------------------------------------------------------
# Shared sub-schemas
# ---------------------------------------------------------------------------

device_info_schema = StructType([
    StructField("device_type",        StringType(),  True),
    StructField("os",                 StringType(),  True),
    StructField("browser",            StringType(),  True),
    StructField("app_version",        StringType(),  True),
    StructField("screen_resolution",  StringType(),  True),
])

geo_location_schema = StructType([
    StructField("country",       StringType(),  True),
    StructField("country_code",  StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("timezone",      StringType(),  True),
])

product_item_schema = StructType([
    StructField("product_id",    StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("subcategory",   StringType(),  True),
    StructField("brand",         StringType(),  True),
    StructField("sku",           StringType(),  True),
    StructField("price",         DoubleType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("discount_pct",  DoubleType(),  True),
    StructField("final_price",   DoubleType(),  True),
])

payment_info_schema = StructType([
    StructField("method",          StringType(), True),
    StructField("provider",        StringType(), True),
    StructField("last_four",       StringType(), True),
    StructField("transaction_id",  StringType(), True),
    StructField("currency",        StringType(), True),
    StructField("status",          StringType(), True),
])

# ---------------------------------------------------------------------------
# Top-level event schemas
# ---------------------------------------------------------------------------

user_activity_schema = StructType([
    StructField("event_id",               StringType(),  True),
    StructField("event_type",             StringType(),  True),
    StructField("timestamp",              StringType(),  True),
    StructField("user_id",                StringType(),  True),
    StructField("session_id",             StringType(),  True),
    StructField("anonymous",              BooleanType(), True),
    StructField("user_segment",           StringType(),  True),
    StructField("account_age_days",       IntegerType(), True),
    StructField("page_url",               StringType(),  True),
    StructField("referrer",               StringType(),  True),
    StructField("search_query",           StringType(),  True),
    StructField("product_id",             StringType(),  True),
    StructField("category",               StringType(),  True),
    StructField("session_duration_sec",   IntegerType(), True),
    StructField("page_views_in_session",  IntegerType(), True),
    StructField("is_bounce",              BooleanType(), True),
    StructField("device",                 device_info_schema,   True),
    StructField("geo",                    geo_location_schema,  True),
    StructField("ab_test_variant",        StringType(),  True),
])

purchase_schema = StructType([
    StructField("event_id",                StringType(),  True),
    StructField("event_type",              StringType(),  True),
    StructField("timestamp",               StringType(),  True),
    StructField("user_id",                 StringType(),  True),
    StructField("session_id",              StringType(),  True),
    StructField("order_id",                StringType(),  True),
    StructField("items",                   ArrayType(product_item_schema), True),
    StructField("item_count",              IntegerType(), True),
    StructField("subtotal",                DoubleType(),  True),
    StructField("tax",                     DoubleType(),  True),
    StructField("shipping_cost",           DoubleType(),  True),
    StructField("discount_amount",         DoubleType(),  True),
    StructField("order_total",             DoubleType(),  True),
    StructField("coupon_code",             StringType(),  True),
    StructField("shipping_method",         StringType(),  True),
    StructField("estimated_delivery_days", IntegerType(), True),
    StructField("warehouse_id",            StringType(),  True),
    StructField("is_gift",                 BooleanType(), True),
    StructField("payment",                 payment_info_schema,  True),
    StructField("device",                  device_info_schema,   True),
    StructField("geo",                     geo_location_schema,  True),
    StructField("customer_order_count",    IntegerType(), True),
    StructField("customer_total_spend",    DoubleType(),  True),
])
