SELECT COUNT(*) AS total_rows
FROM flights_2024_raw;

SELECT MIN(fl_date) AS min_date, MAX(fl_date) AS max_date
FROM flights_2024_raw;

SELECT cancelled, COUNT(*) AS total
FROM flights_2024_raw
GROUP BY cancelled
ORDER BY cancelled;

SELECT diverted, COUNT(*) AS total
FROM flights_2024_raw
GROUP BY diverted
ORDER BY diverted;

SELECT op_unique_carrier, COUNT(*) AS total
FROM flights_2024_raw
GROUP BY op_unique_carrier
ORDER BY total DESC
LIMIT 10;
