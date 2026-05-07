USE team22_projectdb;

DROP TABLE IF EXISTS q7_results;

CREATE EXTERNAL TABLE q7_results (
    scheduled_dep_hour INT,
    avg_dep_delay DOUBLE,
    total_flights BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q7';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q7_results
SELECT
    scheduled_dep_hour,
    AVG(dep_delay) AS avg_dep_delay,
    COUNT(*) AS total_flights
FROM flights_2024_features
WHERE dep_delay IS NOT NULL
GROUP BY scheduled_dep_hour
ORDER BY scheduled_dep_hour;

SELECT * FROM q7_results;
