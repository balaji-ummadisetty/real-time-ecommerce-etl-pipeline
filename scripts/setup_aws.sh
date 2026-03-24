#!/usr/bin/env bash
# setup_aws.sh
# ------------
# Provisions required AWS resources for the e-commerce pipeline:
#   - S3 bucket with folder structure
#   - Glue database and crawlers
#   - IAM role for Glue
#
# Prerequisites: AWS CLI configured with sufficient permissions.
#
# Usage:
#   S3_BUCKET=my-ecommerce-bucket AWS_REGION=us-east-1 ./scripts/setup_aws.sh

set -euo pipefail

S3_BUCKET="${S3_BUCKET:-my-ecommerce-bucket}"
AWS_REGION="${AWS_REGION:-us-east-1}"
GLUE_ROLE="AWSGlueServiceRole-ecommerce"

echo "==> Creating S3 bucket: s3://$S3_BUCKET"
aws s3api create-bucket \
  --bucket "$S3_BUCKET" \
  --region "$AWS_REGION" \
  $([ "$AWS_REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$AWS_REGION" || true)

echo "==> Creating S3 folder placeholders"
for prefix in raw/user_activity raw/purchases processed/user_activity processed/purchases processed/order_items aggregated/sales aggregated/funnel curated/daily_sales curated/category_performance curated/funnel_metrics tmp/redshift; do
  aws s3api put-object --bucket "$S3_BUCKET" --key "$prefix/"
done

echo "==> Creating Glue databases"
aws glue create-database --database-input '{"Name":"ecommerce_processed"}' --region "$AWS_REGION" || echo "Database ecommerce_processed already exists"
aws glue create-database --database-input '{"Name":"ecommerce_curated"}'   --region "$AWS_REGION" || echo "Database ecommerce_curated already exists"

echo "==> Uploading Glue ETL script to S3"
aws s3 cp aws_glue/glue_etl_job.py "s3://$S3_BUCKET/scripts/glue_etl_job.py"

echo "==> Creating Glue crawlers from crawler_config.json"
python3 - <<'EOF'
import json, boto3, sys

with open("aws_glue/crawler_config.json") as f:
    config = json.load(f)

client = boto3.client("glue")
for crawler in config["Crawlers"]:
    try:
        client.create_crawler(**crawler)
        print(f"  Created crawler: {crawler['Name']}")
    except client.exceptions.AlreadyExistsException:
        print(f"  Crawler already exists: {crawler['Name']}")
EOF

echo "==> AWS setup complete."
echo "    S3 bucket : s3://$S3_BUCKET"
echo "    Region    : $AWS_REGION"
