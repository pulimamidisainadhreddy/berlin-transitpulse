-- Number of historical snapshots by station
SELECT queried_station_name, COUNT(*) AS observations
FROM departure_observations
GROUP BY queried_station_name ORDER BY observations DESC;

-- Average reported delay, including negative values, with 5+ observations
SELECT line_name, COUNT(*) AS observations,
       ROUND((AVG(delay_seconds) / 60.0)::numeric, 2) AS avg_delay_minutes
FROM departure_observations
WHERE delay_seconds IS NOT NULL
GROUP BY line_name HAVING COUNT(*) >= 5
ORDER BY avg_delay_minutes DESC;

-- Percent of known-delay snapshots later than planned by 3+ minutes
SELECT line_name, COUNT(*) AS known_delay_observations,
       ROUND(100.0 * AVG(CASE WHEN delay_seconds >= 180 THEN 1.0 ELSE 0.0 END)::numeric, 1)
         AS pct_at_least_3min_late
FROM departure_observations WHERE delay_seconds IS NOT NULL
GROUP BY line_name HAVING COUNT(*) >= 5
ORDER BY pct_at_least_3min_late DESC;

-- Missing delay information by station
SELECT queried_station_name, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE delay_seconds IS NULL) AS missing_delay
FROM departure_observations GROUP BY queried_station_name;
