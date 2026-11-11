from src.collector import extract_departures, normalize


def test_extract_array_and_object():
    item = {"tripId": "abc"}
    assert extract_departures([item]) == [item]
    assert extract_departures({"departures": [item]}) == [item]


def test_normalize_and_null_delay():
    item = {"tripId": "abc", "line": {"name": "S1", "product": "suburban"},
            "plannedWhen": "2026-11-11T09:10:00+01:00", "when": None,
            "delay": None, "stop": {"id": "123"}}
    row = normalize(item, "123", "Demo", "2026-11-11T08:00:00+00:00")
    assert row["delay_seconds"] is None
    assert row["has_realtime_prediction"] is False
    assert row["line_name"] == "S1"


def test_delay_in_seconds():
    row = normalize({"delay": 120, "when": "now"}, "123", "Demo", "now")
    assert row["delay_seconds"] == 120
    assert row["has_realtime_prediction"] is True
