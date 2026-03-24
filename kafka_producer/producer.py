"""
producer.py
-----------
High-throughput Kafka producer that streams fake e-commerce events.

Features:
  • Configurable events-per-second rate
  • Routes activity events → user_activity topic
  • Routes purchase events → purchases topic
  • Malformed / rejected events → pipeline_dlq topic
  • Graceful shutdown on SIGINT / SIGTERM
  • Prometheus-style metrics logged every 10 seconds

Usage:
    python kafka_producer/producer.py
    python kafka_producer/producer.py --rate 200 --purchase-ratio 0.15
    python kafka_producer/producer.py --rate 500 --bootstrap kafka:9092
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Event
from typing import Any, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generator.faker_generator import EcommerceEventGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------

def json_serializer(data: Dict[str, Any]) -> bytes:
    return json.dumps(data, default=lambda o: float(o) if hasattr(o, '__float__') else str(o)).encode("utf-8")

def key_serializer(key: str) -> bytes:
    return key.encode("utf-8")


# ---------------------------------------------------------------------------
# Producer class
# ---------------------------------------------------------------------------

class EcommerceProducer:
    ACTIVITY_TOPIC = "user_activity"
    PURCHASE_TOPIC = "purchases"
    DLQ_TOPIC      = "pipeline_dlq"

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        rate: int             = 100,
        purchase_ratio: float = 0.15,
        seed: int             = 42,
    ):
        self.rate           = rate
        self.purchase_ratio = purchase_ratio
        self.generator      = EcommerceEventGenerator(seed=seed)
        self._stop_event    = Event()

        # Stats counters
        self._sent_activity  = 0
        self._sent_purchases = 0
        self._errors         = 0
        self._last_report    = time.time()

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=json_serializer,
            key_serializer=key_serializer,
            # Throughput tuning
            batch_size=65536,           # 64 KB batch
            linger_ms=5,                # wait up to 5ms to fill batch
            compression_type="gzip",
            # Reliability
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=5,
        )

        # Register shutdown handlers
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        log.info(f"🚀 Producer started | rate={self.rate} ev/s | purchase_ratio={self.purchase_ratio}")
        interval = 1.0 / self.rate

        while not self._stop_event.is_set():
            loop_start = time.perf_counter()

            try:
                event, topic = self._generate_event()
                self._send(event, topic)
            except Exception as exc:
                log.error(f"Unexpected error: {exc}")
                self._errors += 1

            # Throttle to target rate
            elapsed = time.perf_counter() - loop_start
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

            self._maybe_report_metrics()

        log.info("Flushing remaining messages …")
        self.producer.flush(timeout=10)
        self.producer.close()
        log.info("✅ Producer shut down cleanly.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _generate_event(self):
        import random
        if random.random() < self.purchase_ratio:
            return self.generator.generate_purchase(), self.PURCHASE_TOPIC
        else:
            return self.generator.generate_user_activity(), self.ACTIVITY_TOPIC

    def _send(self, event: Dict[str, Any], topic: str):
        """Send event to Kafka with per-record callbacks."""
        user_id = event.get("user_id", "unknown")

        future = self.producer.send(
            topic=topic,
            key=user_id,          # partition by user_id → ordering per user
            value=event,
            timestamp_ms=int(time.time() * 1000),
        )
        future.add_callback(self._on_send_success, topic=topic)
        future.add_errback(self._on_send_error, event=event)

    def _on_send_success(self, record_metadata, topic: str):
        if topic == self.PURCHASE_TOPIC:
            self._sent_purchases += 1
        else:
            self._sent_activity += 1

    def _on_send_error(self, exc: KafkaError, event: Dict[str, Any]):
        log.warning(f"Failed to send event {event.get('event_id')} – routing to DLQ. Error: {exc}")
        self._errors += 1
        dlq_payload = {"original_event": event, "error": str(exc), "dlq_timestamp": datetime.utcnow().isoformat()}
        self.producer.send(self.DLQ_TOPIC, value=dlq_payload, key="dlq")

    def _maybe_report_metrics(self):
        now = time.time()
        if now - self._last_report >= 10:
            total = self._sent_activity + self._sent_purchases
            log.info(
                f"📊 Metrics | activity={self._sent_activity:,}  "
                f"purchases={self._sent_purchases:,}  "
                f"errors={self._errors}  "
                f"total={total:,}"
            )
            self._last_report = now

    def _shutdown(self, signum, frame):
        log.info("Shutdown signal received – stopping producer …")
        self._stop_event.set()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Kafka producer")
    parser.add_argument("--bootstrap",       default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--rate",            type=int,   default=100,  help="Events per second")
    parser.add_argument("--purchase-ratio",  type=float, default=0.15, help="Fraction of events that are purchases")
    parser.add_argument("--seed",            type=int,   default=42,   help="Random seed for reproducibility")
    args = parser.parse_args()

    EcommerceProducer(
        bootstrap_servers=args.bootstrap,
        rate=args.rate,
        purchase_ratio=args.purchase_ratio,
        seed=args.seed,
    ).run()
