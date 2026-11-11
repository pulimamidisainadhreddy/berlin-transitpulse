CREATE TABLE IF NOT EXISTS departure_observations (
    observation_hash TEXT PRIMARY KEY,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    queried_station_id TEXT NOT NULL,
    queried_station_name TEXT NOT NULL,
    actual_stop_id TEXT,
    actual_stop_name TEXT,
    trip_id TEXT,
    line_name TEXT,
    transport_type TEXT,
    direction TEXT,
    planned_departure TIMESTAMPTZ,
    predicted_departure TIMESTAMPTZ,
    delay_seconds DOUBLE PRECISION,
    cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    has_realtime_prediction BOOLEAN NOT NULL DEFAULT FALSE,
    imported_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_departures_observed ON departure_observations(observed_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_departures_line ON departure_observations(line_name);
CREATE INDEX IF NOT EXISTS idx_departures_station ON departure_observations(queried_station_id);
CREATE INDEX IF NOT EXISTS idx_departures_trip ON departure_observations(trip_id);
