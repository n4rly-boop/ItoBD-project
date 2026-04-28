USE team22_projectdb;

DROP TABLE IF EXISTS q8_results;

CREATE EXTERNAL TABLE q8_results (
    is_weekend INT,
    avg_arr_delay DOUBLE,
    avg_dep_delay DOUBLE,
    total_flights BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q8';

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q8_results
SELECT
    is_weekend,
    AVG(arr_delay) AS avg_arr_delay,
    AVG(dep_delay) AS avg_dep_delay,
    COUNT(*) AS total_flights
FROM flights_2024_features
WHERE arr_delay IS NOT NULL AND dep_delay IS NOT NULL
GROUP BY is_weekend
ORDER BY is_weekend;

SELECT * FROM q8_results;
