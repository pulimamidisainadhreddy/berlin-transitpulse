# Phase 3A — Kafka events to PostgreSQL

**Designed as an additive extension to your existing Phase 2 repository.** It does not recreate PostgreSQL and does not delete the `postgres_data` volume. This step uses a Python Kafka consumer for end-to-end validation. PySpark streaming is a separate next step.

## Install
From the original repository root, with `.venv` activated:

```bash
python -m pip install 'confluent-kafka>=2.5,<3'
docker compose -f docker-compose.kafka.yml up -d
```

The first Kafka start can take time. Verify:

```bash
docker compose -f docker-compose.kafka.yml ps
```

Create a topic with three partitions:

```bash
docker compose -f docker-compose.kafka.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic transit.departures.raw --partitions 3 --replication-factor 1
```

## Run
In Terminal A (leave running):

```bash
python -m src.kafka_consumer
```

In Terminal B:

```bash
python -m src.kafka_producer --once
```

Check PostgreSQL row counts:

```bash
docker compose exec postgres psql -U transit -d transitpulse -c 'SELECT COUNT(*) FROM departure_observations;'
```

Verify the Kafka topic:

```bash
docker compose -f docker-compose.kafka.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic transit.departures.raw
```

## Understand the behavior
- The producer polls the VBB API for departures and sends normalized snapshots to Kafka. It does **not** append JSONL; the earlier collector still works independently.
- The consumer uses the existing database `INSERT ... ON CONFLICT DO NOTHING`. Replayed identical messages will not duplicate rows.
- The consumer commits Kafka offsets only after a database transaction; it stops on malformed messages rather than silently losing them.
- In this phase, the consumer is Python, **not PySpark**. Next step: replace/augment it with Spark Structured Streaming, schema validation, checkpoints and data quality monitoring.
- This is intended for local development and at-least-once processing; PostgreSQL uniqueness makes replays safe for identical snapshots.

## Security and Git hygiene
The repository currently tracks a generated `data/departures_2026-11-11.jsonl` file. To stop tracking it while keeping your local copy:

```bash
git rm --cached data/departures_2026-11-11.jsonl
printf '\ndata/*.jsonl\n' >> .gitignore
```

Commit the removal when you are ready. Existing commits still contain that file; stop tracking it now to avoid further history growth.

## Stop safely

```bash
docker compose -f docker-compose.kafka.yml down
```

**Do not run `docker compose down -v`** because `-v` removes volumes (including stored Kafka data, or PostgreSQL data if used with the original compose file).
