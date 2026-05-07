USE team22_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results (
    origin STRING,
    avg_dep_delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

SET hive.resultset.use.unique.column.names = false;

INSERT OVERWRITE TABLE q2_results
SELECT origin, AVG(dep_delay) AS avg_dep_delay
FROM flights_2024_features
WHERE dep_delay IS NOT NULL
GROUP BY origin
ORDER BY avg_dep_delay DESC
LIMIT 20;

SELECT * FROM q2_results;
