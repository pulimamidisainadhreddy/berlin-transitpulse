"""Basic CLI analytics: report delays in the collected snapshots."""
import json
from collections import defaultdict
from datetime import date
from pathlib import Path


def main() -> None:
    path = Path("data/processed") / f"departures_{date.today().isoformat()}.jsonl"
    if not path.exists():
        print("No processed data for today yet. Run python -m src.collector --once")
        return
    by_line = defaultdict(list)
    total = 0
    with path.open(encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            total += 1
            if row.get("delay_seconds") is not None:
                by_line[row.get("line_name") or "UNKNOWN"].append(row["delay_seconds"])
    print(f"Snapshot rows collected: {total} (includes repeated observations)")
    print("Line | observations with delay | average reported delay (minutes)")
    for route, values in sorted(by_line.items(), key=lambda x: -len(x[1])):
        avg = sum(values) / len(values) / 60
        print(f"{route} | {len(values)} | {avg:.1f}")
    print("Note: averages are snapshot-weighted; not unique trips or actual outcomes.")


if __name__ == "__main__":
    main()
