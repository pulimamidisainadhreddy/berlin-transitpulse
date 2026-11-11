# Berlin TransitPulse — Phase 1

Original historical dataset built by polling the Berlin/Brandenburg VBB departures API. Unlike a static public dataset, every snapshot is collected, timestamped, and stored by this project.

## Setup (macOS, Python 3.11+)
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
python -m src.collector --once
python -m src.report
pytest -q
```

To keep collecting every five minutes while the terminal is running:
```bash
python -m src.collector --loop
```
Stop with Ctrl+C.

## What gets saved?
- `data/raw/departures_YYYY-MM-DD.jsonl`: raw API responses with timestamps.
- `data/processed/departures_YYYY-MM-DD.jsonl`: normalized departure observations.

Each poll can observe the **same trip multiple times**, intentionally. This enables comparing predicted times across snapshots. These are *observations*, not unique completed journeys; do not calculate actual punctuality from them.

## API
https://v6.vbb.transport.rest/ — community-maintained, no key required, rate-limited. Start with just two stations; use responsibly and review API/data rights before publishing raw data or running continuously.

## Caveats
- API predictions can be unavailable, delayed, or inaccurate; null delays are not zero.
- The job is local and only collects while running. It is not yet a production service.
- Network access is required to collect real data.
- Stop IDs, field names, and access policies may evolve; inspect logs if requests fail.
- Next phases: PostgreSQL schema and quality tests; Kafka; PySpark; Airflow; S3; dashboard.

## Phase 2 (PostgreSQL)
See [PHASE2.md](PHASE2.md) for database setup, idempotent import and SQL analytics.
