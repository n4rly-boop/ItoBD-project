USE team22_projectdb;

DROP TABLE IF EXISTS q9_results;

CREATE EXTERNAL TABLE q9_results (
    dist_bin STRING,
    avg_arr_delay DOUBLE,
    flight_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q9';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q9_results
SELECT
    CASE
        WHEN distance < 500 THEN '0-499 mi'
        WHEN distance < 1000 THEN '500-999 mi'
        WHEN distance < 2000 THEN '1000-1999 mi'
        ELSE '2000+ mi'
    END AS dist_bin,
    AVG(arr_delay) AS avg_arr_delay,
    COUNT(*) AS flight_count
FROM flights_2024_features
WHERE arr_delay IS NOT NULL AND distance IS NOT NULL
GROUP BY
    CASE
        WHEN distance < 500 THEN '0-499 mi'
        WHEN distance < 1000 THEN '500-999 mi'
        WHEN distance < 2000 THEN '1000-1999 mi'
        ELSE '2000+ mi'
    END
ORDER BY avg_arr_delay DESC;

SELECT * FROM q9_results;