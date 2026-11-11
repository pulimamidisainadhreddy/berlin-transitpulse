"""Consume Kafka departure events into the existing PostgreSQL table.

Commit Kafka offsets only after the database transaction commits.
"""
import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
import psycopg

from src.db_import import INSERT, convert

load_dotenv()
LOG = logging.getLogger("transitpulse.kafka.consumer")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("KAFKA_CONSUMER_GROUP", "transitpulse-postgres-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    topic = os.getenv("KAFKA_TOPIC", "transit.departures.raw")
    consumer.subscribe([topic])
    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    db_url = os.getenv("DATABASE_URL", "postgresql://transit:transit_local_dev@localhost:5434/transitpulse")
    try:
        with psycopg.connect(db_url) as conn:
            while running:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                try:
                    row = convert(json.loads(message.value().decode("utf-8")))
                except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError) as exc:
                    # Stop instead of losing a malformed message; add DLQ in a later phase.
                    LOG.error("Invalid record at offset %s: %s", message.offset(), exc)
                    raise
                with conn.transaction():
                    with conn.cursor() as cursor:
                        cursor.execute(INSERT, row)
                        inserted = cursor.rowcount
                consumer.commit(message=message, asynchronous=False)
                LOG.info("partition=%s offset=%s inserted=%s", message.partition(), message.offset(), inserted)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
