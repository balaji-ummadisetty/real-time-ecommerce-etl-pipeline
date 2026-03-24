"""
topic_setup.py
--------------
Creates required Kafka topics before the pipeline starts.

Usage:
    python kafka_producer/topic_setup.py
    python kafka_producer/topic_setup.py --bootstrap kafka:9092
"""

from __future__ import annotations

import argparse
import logging
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = [
    NewTopic(name="user_activity", num_partitions=6, replication_factor=1),
    NewTopic(name="purchases",     num_partitions=6, replication_factor=1),
    NewTopic(name="pipeline_dlq",  num_partitions=2, replication_factor=1),
]


def create_topics(bootstrap_servers: str) -> None:
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="topic-setup")
    try:
        client.create_topics(new_topics=TOPICS, validate_only=False)
        for t in TOPICS:
            log.info("Created topic: %s (partitions=%d)", t.name, t.num_partitions)
    except TopicAlreadyExistsError:
        log.info("Topics already exist — skipping creation.")
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topics for the e-commerce pipeline")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()
    create_topics(args.bootstrap)
