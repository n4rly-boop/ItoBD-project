DROP DATABASE IF EXISTS team22_projectdb CASCADE;
CREATE DATABASE team22_projectdb LOCATION 'project/hive/warehouse';

USE team22_projectdb;

DROP TABLE IF EXISTS flights_2024_raw_ext;

CREATE EXTERNAL TABLE flights_2024_raw_ext
STORED AS AVRO
LOCATION 'project/warehouse/flights_2024_raw'
TBLPROPERTIES (
  'avro.schema.url'='project/warehouse/avsc/flights_2024_raw.avsc'
);

SHOW DATABASES;
USE team22_projectdb;
SHOW TABLES;
DESCRIBE FORMATTED flights_2024_raw_ext;
SELECT * FROM flights_2024_raw_ext LIMIT 5;
