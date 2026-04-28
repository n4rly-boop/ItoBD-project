USE team22_projectdb;

DROP TABLE IF EXISTS q10_results;

CREATE EXTERNAL TABLE q10_results (
    state STRING,
    avg_dep_delay DOUBLE,
    flight_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q10';

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q10_results
SELECT
    origin_state_nm AS state,
    AVG(dep_delay) AS avg_dep_delay,
    COUNT(*) AS flight_count
FROM flights_2024_features
WHERE dep_delay IS NOT NULL
GROUP BY origin_state_nm
ORDER BY avg_dep_delay DESC
LIMIT 20;

SELECT * FROM q10_results;
