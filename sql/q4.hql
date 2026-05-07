USE team22_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results (
    cancellation_code STRING,
    cancel_count BIGINT,
    cancel_pct DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q4_results
SELECT
    cancellation_code,
    COUNT(*) AS cancel_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS cancel_pct
FROM flights_2024_features
WHERE cancelled = 1
GROUP BY cancellation_code
ORDER BY cancel_count DESC;

SELECT * FROM q4_results;
