# Phase 2: PostgreSQL historical observations

Run from the repository root, with Docker Desktop open.

1. `docker compose up -d`
2. `docker compose ps`  # wait until postgres is healthy
3. `source .venv/bin/activate`  # macOS; use existing venv if already active
4. `python -m pip install -r requirements.txt`
5. If your `.env` already exists, add `DATABASE_URL=postgresql://transit:transit_local_dev@localhost:5434/transitpulse` to it.
6. `python -m src.db_import`
7. Re-run `python -m src.db_import` and confirm `inserted=0` (provided files are unchanged).
8. `docker compose exec -T postgres psql -U transit -d transitpulse -c 'SELECT COUNT(*) FROM departure_observations;'`
9. `docker compose exec -T postgres psql -U transit -d transitpulse < sql/002_analysis.sql`
10. `python -m pytest -q`

## Design
Each source row is a *snapshot observation*, not a unique journey. We derive a SHA256 hash
from the full normalized JSON to make ingestion idempotent. Re-observing the same trip at a
new collection timestamp is intentionally retained as a new row. Negative delays are kept.
Missing delays remain NULL, and the average isn't treated as actual punctuality.

Postgres uses Docker volume `postgres_data`. **Do not run `docker compose down -v`** unless
intending to delete the database. Source JSONL stays in `data/processed` (gitignored).
The password and port are local development defaults; change credentials before deployment.

## Known limitations
- Repeated observations are not independent real-world trips.
- A row with no delay is NULL, not automatically 0.
- File input errors and DB constraints halt transaction; malformed JSON lines are logged and skipped.
- If source data is large, switch row-by-row insertion to batches / COPY and add ingestion tracking.
