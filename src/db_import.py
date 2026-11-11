"""Import locally collected JSONL snapshots into PostgreSQL, safely repeatable."""
import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import psycopg

load_dotenv()
LOGGER = logging.getLogger("transitpulse.db")
COLUMNS = (
    "observation_hash", "observed_at_utc", "queried_station_id", "queried_station_name",
    "actual_stop_id", "actual_stop_name", "trip_id", "line_name", "transport_type",
    "direction", "planned_departure", "predicted_departure", "delay_seconds",
    "cancelled", "has_realtime_prediction",
)
INSERT = (
    f"INSERT INTO departure_observations ({', '.join(COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(COLUMNS))}) "
    "ON CONFLICT (observation_hash) DO NOTHING"
)


def observation_hash(row: dict) -> str:
    """Hash complete normalized record so re-importing the same snapshot is safe."""
    canonical = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_timestamp(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Expected ISO timestamp string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp must include timezone")
    return parsed


def convert(row: dict) -> tuple:
    required = ("observed_at_utc", "queried_station_id", "queried_station_name")
    if any(not row.get(key) for key in required):
        raise ValueError("Missing required observation fields")
    return (
        observation_hash(row), parse_timestamp(row["observed_at_utc"]),
        str(row["queried_station_id"]), row["queried_station_name"],
        row.get("actual_stop_id"), row.get("actual_stop_name"), row.get("trip_id"),
        row.get("line_name"), row.get("transport_type"), row.get("direction"),
        parse_timestamp(row.get("planned_departure")),
        parse_timestamp(row.get("predicted_departure")), row.get("delay_seconds"),
        bool(row.get("cancelled", False)), bool(row.get("has_realtime_prediction", False)),
    )


def import_files(paths: list[Path], url: str) -> tuple[int, int, int]:
    total = inserted = invalid = 0
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            for path in paths:
                with path.open(encoding="utf-8") as handle:
                    for line_number, line in enumerate(handle, 1):
                        if not line.strip():
                            continue
                        total += 1
                        try:
                            row = convert(json.loads(line))
                        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                            invalid += 1
                            LOGGER.warning("skipped invalid %s:%s: %s", path, line_number, exc)
                            continue
                        cur.execute(INSERT, row)
                        inserted += cur.rowcount
            # Transaction commits only after all SQL statements succeed.
    return total, inserted, invalid


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path("data/processed"),
                        help="Directory of JSONL files or a single JSONL file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    paths = sorted(args.path.glob("departures_*.jsonl")) if args.path.is_dir() else [args.path]
    paths = [path for path in paths if path.is_file()]
    if not paths:
        raise SystemExit(f"No input files found at {args.path}")
    url = os.getenv("DATABASE_URL", "postgresql://transit:transit_local_dev@localhost:5434/transitpulse")
    total, inserted, invalid = import_files(paths, url)
    print(f"Files={len(paths)} rows_read={total} inserted={inserted} duplicates={total-inserted-invalid} invalid={invalid}")


if __name__ == "__main__":
    main()
