USE team22_projectdb;

SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS flights_2024_part_buck;

CREATE EXTERNAL TABLE flights_2024_part_buck (
    flight_id BIGINT,
    year INT,
    day_of_month INT,
    day_of_week INT,
    fl_date DATE,
    op_unique_carrier STRING,
    op_carrier_fl_num DOUBLE,
    origin STRING,
    origin_city_name STRING,
    origin_state_nm STRING,
    dest STRING,
    dest_city_name STRING,
    dest_state_nm STRING,
    crs_dep_time INT,
    dep_time DOUBLE,
    dep_delay DOUBLE,
    taxi_out DOUBLE,
    wheels_off DOUBLE,
    wheels_on DOUBLE,
    taxi_in DOUBLE,
    crs_arr_time INT,
    arr_time DOUBLE,
    arr_delay DOUBLE,
    cancelled INT,
    cancellation_code STRING,
    diverted INT,
    crs_elapsed_time DOUBLE,
    actual_elapsed_time DOUBLE,
    air_time DOUBLE,
    distance DOUBLE,
    carrier_delay INT,
    weather_delay INT,
    nas_delay INT,
    security_delay INT,
    late_aircraft_delay INT
)
PARTITIONED BY (month INT)
CLUSTERED BY (op_unique_carrier) INTO 16 BUCKETS
STORED AS PARQUET
LOCATION 'project/hive/warehouse/flights_2024_part_buck';

INSERT OVERWRITE TABLE flights_2024_part_buck
PARTITION (month)
SELECT
    flight_id,
    year,
    day_of_month,
    day_of_week,
    TO_DATE(FROM_UNIXTIME(CAST(fl_date / 1000 AS BIGINT))) AS fl_date,
    op_unique_carrier,
    CAST(op_carrier_fl_num AS DOUBLE) AS op_carrier_fl_num,
    origin,
    origin_city_name,
    origin_state_nm,
    dest,
    dest_city_name,
    dest_state_nm,
    crs_dep_time,
    CAST(dep_time AS DOUBLE) AS dep_time,
    CAST(dep_delay AS DOUBLE) AS dep_delay,
    CAST(taxi_out AS DOUBLE) AS taxi_out,
    CAST(wheels_off AS DOUBLE) AS wheels_off,
    CAST(wheels_on AS DOUBLE) AS wheels_on,
    CAST(taxi_in AS DOUBLE) AS taxi_in,
    crs_arr_time,
    CAST(arr_time AS DOUBLE) AS arr_time,
    CAST(arr_delay AS DOUBLE) AS arr_delay,
    cancelled,
    cancellation_code,
    diverted,
    CAST(crs_elapsed_time AS DOUBLE) AS crs_elapsed_time,
    CAST(actual_elapsed_time AS DOUBLE) AS actual_elapsed_time,
    CAST(air_time AS DOUBLE) AS air_time,
    CAST(distance AS DOUBLE) AS distance,
    carrier_delay,
    weather_delay,
    nas_delay,
    security_delay,
    late_aircraft_delay,
    month
FROM flights_2024_raw_ext;

SHOW TABLES;
SHOW PARTITIONS flights_2024_part_buck;
DESCRIBE FORMATTED flights_2024_part_buck;
SELECT month, COUNT(*) AS total_rows
FROM flights_2024_part_buck
GROUP BY month
ORDER BY month
LIMIT 12;

SELECT op_unique_carrier, COUNT(*) AS total_rows
FROM flights_2024_part_buck
WHERE month = 1
GROUP BY op_unique_carrier
ORDER BY total_rows DESC
LIMIT 10;
