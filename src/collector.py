"""Collect timestamped Berlin public transport departure snapshots.

Use `python -m src.collector --once` first; no Kafka or DB needed for phase 1.
"""
import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
LOGGER = logging.getLogger("transitpulse")
BASE = os.getenv("VBB_API_BASE", "https://v6.vbb.transport.rest").rstrip("/")
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def stations_from_env() -> list[tuple[str, str]]:
    configured = os.getenv("STATIONS", "900058101:S Südkreuz,900017101:U Mehringdamm")
    stations = []
    for part in configured.split(","):
        if ":" not in part:
            raise ValueError(f"Invalid STATIONS entry: {part!r}. Use ID:Station name")
        station_id, name = part.split(":", 1)
        if not station_id.strip().isdigit() or not name.strip():
            raise ValueError(f"Invalid station entry: {part!r}")
        stations.append((station_id.strip(), name.strip()))
    if not stations:
        raise ValueError("At least one station is required")
    return stations


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "BerlinTransitPulse-Portfolio/0.1 (educational)"})
    return session


def extract_departures(payload: Any) -> list[dict]:
    # Some API deployments respond with an array, others wrap it in a dict.
    if isinstance(payload, list):
        departures = payload
    elif isinstance(payload, dict) and isinstance(payload.get("departures"), list):
        departures = payload["departures"]
    else:
        raise ValueError("Unexpected departures response structure")
    return [item for item in departures if isinstance(item, dict)]


def normalize(item: dict, station_id: str, station_name: str, observed_at: str) -> dict:
    line = item.get("line") or {}
    stop = item.get("stop") or {}
    delay = item.get("delay")
    return {
        "observed_at_utc": observed_at,
        "queried_station_id": station_id,
        "queried_station_name": station_name,
        "actual_stop_id": stop.get("id"),
        "actual_stop_name": stop.get("name"),
        "trip_id": item.get("tripId"),
        "line_name": line.get("name"),
        "transport_type": line.get("product"),
        "direction": item.get("direction"),
        "planned_departure": item.get("plannedWhen"),
        "predicted_departure": item.get("when"),
        "delay_seconds": delay if isinstance(delay, (int, float)) and not isinstance(delay, bool) else None,
        "cancelled": item.get("cancelled") is True,
        "has_realtime_prediction": item.get("when") is not None and delay is not None,
    }


def append_jsonl(path: Path, objects: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        for obj in objects:
            output.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def collect_once(session: requests.Session, stations: list[tuple[str, str]]) -> int:
    total = 0
    for station_id, station_name in stations:
        try:
            response = session.get(
                f"{BASE}/stops/{station_id}/departures",
                params={"duration": 20, "results": 30, "remarks": "false"},
                timeout=float(os.getenv("REQUEST_TIMEOUT", "20")),
            )
            response.raise_for_status()
            departures = extract_departures(response.json())
            observed_at = datetime.now(timezone.utc).isoformat()
            utc_day = observed_at[:10]
            # Raw per-request envelope preserves the API payload and collection timestamp.
            append_jsonl(RAW_DIR / f"departures_{utc_day}.jsonl", [{
                "observed_at_utc": observed_at,
                "queried_station_id": station_id,
                "queried_station_name": station_name,
                "api_response": response.json(),
            }])
            rows = [normalize(item, station_id, station_name, observed_at) for item in departures]
            append_jsonl(PROCESSED_DIR / f"departures_{utc_day}.jsonl", rows)
            total += len(rows)
            LOGGER.info("station=%s departures=%s", station_name, len(rows))
        except (requests.RequestException, ValueError) as exc:
            LOGGER.error("station=%s collection failed: %s", station_name, exc)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect VBB departure predictions")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="One collection cycle")
    mode.add_argument("--loop", action="store_true", help="Keep collecting every POLL_SECONDS")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stations = stations_from_env()
    seconds = int(os.getenv("POLL_SECONDS", "300"))
    if seconds < 60:
        raise ValueError("POLL_SECONDS must be at least 60 to limit API usage")
    with build_session() as session:
        while True:
            total = collect_once(session, stations)
            LOGGER.info("cycle complete: %d normalized rows", total)
            if args.once:
                break
            try:
                time.sleep(seconds)
            except KeyboardInterrupt:
                LOGGER.info("collector stopped")
                break


if __name__ == "__main__":
    main()
