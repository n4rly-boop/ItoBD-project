USE team22_projectdb;

DROP TABLE IF EXISTS flights_2024_raw_ext;

SHOW TABLES;

DESCRIBE FORMATTED flights_2024_part_buck;

SELECT COUNT(*) AS total_rows
FROM flights_2024_part_buck;

SELECT MIN(fl_date) AS min_date, MAX(fl_date) AS max_date
FROM flights_2024_part_buck;
