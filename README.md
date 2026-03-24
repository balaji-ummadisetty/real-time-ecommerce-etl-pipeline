# Real-Time E-Commerce Data Pipeline

A personal practice project to learn end-to-end streaming data engineering using Kafka, PySpark, and AWS.

Built a real-time ELT pipeline that ingests user activity and purchase events through Kafka, processes them with PySpark Structured Streaming, stores them in a layered S3 data lake, and serves analytics via Athena and Redshift.

---

## Architecture

```
Faker Generator
     │
     ▼
Kafka Producer ──► Kafka Topics (user_activity, purchases, dlq)
                          │
                          ▼
              PySpark Structured Streaming
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
     Raw (S3)       Processed (S3)   Aggregated (S3)
     JSON           Parquet          Parquet (windowed)
          └───────────────┼───────────────┘
                          ▼
                   AWS Glue Crawler
                   AWS Glue ETL
                          │
               ┌──────────┴──────────┐
               ▼                     ▼
          Athena                  Redshift
       (ad-hoc SQL)           (warehouse reports)
```

---

## Project Structure

```
├── config/
│   └── config.yaml               # Kafka, AWS, Spark settings
├── data_generator/
│   ├── faker_generator.py        # Generates fake user activity & purchase events
│   └── event_schemas.py          # Pydantic models for event validation
├── kafka_producer/
│   ├── producer.py               # Publishes events to Kafka topics
│   └── topic_setup.py            # Creates Kafka topics
├── spark_streaming/
│   ├── streaming_job.py          # Main PySpark Structured Streaming job
│   ├── transformations.py        # Enrichment and aggregation logic
│   └── schema_definitions.py     # Spark schemas for Kafka messages
├── aws_glue/
│   ├── glue_etl_job.py           # Glue ETL script (processed → curated)
│   └── crawler_config.json       # Glue Crawler config for schema discovery
├── analytics/
│   ├── athena_queries.sql        # Sample ad-hoc queries
│   └── redshift_schema.sql       # Table DDL and reporting queries
├── docker/
│   └── docker-compose.yml        # Kafka (KRaft mode) + Kafka UI
├── scripts/
│   ├── run_pipeline.sh           # One-command pipeline launcher
│   └── setup_aws.sh              # AWS resource provisioning
└── sample_output/
    ├── user_activity_sample.json
    └── purchase_sample.json
```

---

## Data Layers (S3)

| Layer | Path | Format | Partition |
|-------|------|--------|-----------|
| Raw | `s3://bucket/raw/` | JSON | `year/month/day/hour` |
| Processed | `s3://bucket/processed/` | Parquet | `event_type/date` |
| Aggregated | `s3://bucket/aggregated/` | Parquet | `date` |
| Curated | `s3://bucket/curated/` | Parquet | `date` |

---

## Running Locally

### Prerequisites

- Docker
- Python 3.9+
- Java 11+ (for Spark)
- AWS credentials configured (for S3 writes)

```bash
pip install -r requirements.txt
```

### Step 1 — Start Kafka

```bash
docker compose -f docker/docker-compose.yml up -d
```

Kafka UI is available at http://localhost:8080

### Step 2 — Create Kafka Topics

```bash
python kafka_producer/topic_setup.py
```

### Step 3 — Start the Producer

```bash
python kafka_producer/producer.py --rate 100 --purchase-ratio 0.15
```

### Step 4 — Start Spark Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.261 \
  spark_streaming/streaming_job.py
```

### Or run everything at once

```bash
./scripts/run_pipeline.sh --rate 100
```

---

## AWS Setup (Optional)

Provision S3, Glue, and Redshift resources:

```bash
./scripts/setup_aws.sh
```

Then run the Glue crawler and ETL job:

```bash
# Discover schema from raw S3 data
aws glue start-crawler --name ecommerce-raw-crawler

# Run ETL to produce curated layer
aws glue start-job-run --job-name ecommerce-etl-job

# Query with Athena (see analytics/athena_queries.sql)
# Load into Redshift (see analytics/redshift_schema.sql)
```
