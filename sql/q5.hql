USE team22_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results (
    day_of_week INT,
    avg_arr_delay DOUBLE,
    avg_dep_delay DOUBLE,
    total_flights BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q5_results
SELECT
    day_of_week,
    AVG(arr_delay) AS avg_arr_delay,
    AVG(dep_delay) AS avg_dep_delay,
    COUNT(*) AS total_flights
FROM flights_2024_features
WHERE arr_delay IS NOT NULL AND dep_delay IS NOT NULL
GROUP BY day_of_week
ORDER BY day_of_week;

SELECT * FROM q5_results;
