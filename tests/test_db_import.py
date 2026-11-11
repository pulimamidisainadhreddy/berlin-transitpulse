from datetime import datetime
import pytest
from src.db_import import convert, observation_hash, parse_timestamp


def test_hash_is_independent_of_key_order():
    assert observation_hash({"a": 1, "b": 2}) == observation_hash({"b": 2, "a": 1})


def test_timestamp_requires_timezone():
    with pytest.raises(ValueError):
        parse_timestamp("2026-11-11T10:00:00")


def test_convert_preserves_negative_delay():
    value = convert({"observed_at_utc": "2026-11-11T09:54:54+00:00",
                     "queried_station_id": "1", "queried_station_name": "Test",
                     "delay_seconds": -100})
    assert value[12] == -100
