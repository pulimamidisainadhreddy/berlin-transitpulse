"""Collect VBB departures and publish normalized observations to Kafka.

Run from the repository root: python -m src.kafka_producer --once
"""
import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

from src.collector import BASE, build_session, extract_departures, normalize, stations_from_env

load_dotenv()
LOG = logging.getLogger("transitpulse.kafka.producer")


def publish_once(session, producer: Producer, topic: str) -> int:
    total = 0
    for station_id, station_name in stations_from_env():
        try:
            response = session.get(
                f"{BASE}/stops/{station_id}/departures",
                params={"duration": 20, "results": 30, "remarks": "false"},
                timeout=float(os.getenv("REQUEST_TIMEOUT", "20")),
            )
            response.raise_for_status()
            departures = extract_departures(response.json())
            observed_at = datetime.now(timezone.utc).isoformat()
            for item in departures:
                row = normalize(item, station_id, station_name, observed_at)
                payload = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                # Keying by station keeps records for one station ordered within a partition.
                while True:
                    try:
                        producer.produce(topic, key=station_id.encode(), value=payload)
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
                total += 1
            LOG.info("station=%s queued=%s", station_name, len(departures))
        except Exception:
            LOG.exception("station=%s collection failed", station_name)
    # flush returns number of messages still pending after timeout; never report as success.
    pending = producer.flush(30)
    if pending:
        raise RuntimeError(f"Kafka delivery incomplete: {pending} messages remain")
    return total


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    poll_seconds = int(os.getenv("POLL_SECONDS", "300"))
    if poll_seconds < 60:
        raise ValueError("POLL_SECONDS must be at least 60")
    topic = os.getenv("KAFKA_TOPIC", "transit.departures.raw")
    delivery_errors = []
    def delivery_report(err, msg):
        if err is not None:
            delivery_errors.append(str(err))
            LOG.error("delivery failed: %s", err)
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "on_delivery": delivery_report,
    })
    with build_session() as session:
        while True:
            count = publish_once(session, producer, topic)
            if delivery_errors:
                raise RuntimeError(f"Kafka delivery errors: {delivery_errors[:3]}")
            LOG.info("cycle published=%d topic=%s", count, topic)
            if args.once:
                break
            try:
                time.sleep(poll_seconds)
            except KeyboardInterrupt:
                break

if __name__ == "__main__":
    main()
