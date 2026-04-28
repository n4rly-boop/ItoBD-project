USE team22_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results (
    carrier STRING,
    flight_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q1';

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT op_unique_carrier AS carrier, COUNT(*) AS flight_count
FROM flights_2024_features
GROUP BY op_unique_carrier
ORDER BY flight_count DESC;

SELECT * FROM q1_results;
