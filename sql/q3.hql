USE team22_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results (
    month INT,
    flight_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q3_results
SELECT month, COUNT(*) AS flight_count
FROM flights_2024_features
GROUP BY month
ORDER BY month;

SELECT * FROM q3_results;
