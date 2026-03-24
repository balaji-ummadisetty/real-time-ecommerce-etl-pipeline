#!/usr/bin/env bash
# run_pipeline.sh
# ---------------
# One-command launcher: starts Kafka, creates topics, launches the producer
# and the Spark streaming job.
#
# Usage:
#   ./scripts/run_pipeline.sh [--rate 200] [--purchase-ratio 0.15]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

BOOTSTRAP="${BOOTSTRAP_SERVERS:-localhost:9092}"
RATE="${RATE:-100}"
PURCHASE_RATIO="${PURCHASE_RATIO:-0.15}"

echo "==> Starting Kafka & Zookeeper (docker/docker-compose.yml)"
docker compose -f docker/docker-compose.yml up -d

echo "==> Waiting for Kafka to be ready..."
sleep 10

echo "==> Creating Kafka topics"
python kafka_producer/topic_setup.py --bootstrap "$BOOTSTRAP"

echo "==> Starting Kafka producer (background)"
python kafka_producer/producer.py \
  --bootstrap "$BOOTSTRAP" \
  --rate "$RATE" \
  --purchase-ratio "$PURCHASE_RATIO" &
PRODUCER_PID=$!
echo "    Producer PID: $PRODUCER_PID"

echo "==> Submitting Spark Structured Streaming job"

SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.261"

spark-submit \
  --packages "$SPARK_PACKAGES" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  spark_streaming/streaming_job.py

# Clean up producer on exit
trap "kill $PRODUCER_PID 2>/dev/null || true" EXIT
