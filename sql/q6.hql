USE team22_projectdb;

DROP TABLE IF EXISTS q6_results;

CREATE EXTERNAL TABLE q6_results (
    route STRING,
    flight_count BIGINT,
    avg_distance DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q6';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q6_results
SELECT
    CONCAT(origin, ' → ', dest) AS route,
    COUNT(*) AS flight_count,
    AVG(distance) AS avg_distance
FROM flights_2024_features
GROUP BY origin, dest
ORDER BY flight_count DESC
LIMIT 15;

SELECT * FROM q6_results;
