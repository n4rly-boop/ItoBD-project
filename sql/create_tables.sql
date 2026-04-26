START TRANSACTION;

DROP TABLE IF EXISTS flights_2024_raw CASCADE;

CREATE TABLE IF NOT EXISTS flights_2024_raw (
    flight_id BIGSERIAL PRIMARY KEY,

    year INTEGER NOT NULL CHECK (year BETWEEN 2024 AND 2024),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day_of_month SMALLINT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),

    fl_date DATE NOT NULL,

    op_unique_carrier VARCHAR(10) NOT NULL,
    op_carrier_fl_num NUMERIC(10,1) CHECK (op_carrier_fl_num > 0),

    origin VARCHAR(10) NOT NULL,
    origin_city_name VARCHAR(128) NOT NULL,
    origin_state_nm VARCHAR(128) NOT NULL,

    dest VARCHAR(10) NOT NULL,
    dest_city_name VARCHAR(128) NOT NULL,
    dest_state_nm VARCHAR(128) NOT NULL,

    crs_dep_time INTEGER NOT NULL CHECK (crs_dep_time BETWEEN 0 AND 2400),
    dep_time NUMERIC(10,1),
    dep_delay NUMERIC(10,2),

    taxi_out NUMERIC(10,2),
    wheels_off NUMERIC(10,1),
    wheels_on NUMERIC(10,1),
    taxi_in NUMERIC(10,2),

    crs_arr_time INTEGER NOT NULL CHECK (crs_arr_time BETWEEN 0 AND 2400),
    arr_time NUMERIC(10,1),
    arr_delay NUMERIC(10,2),

    cancelled SMALLINT NOT NULL CHECK (cancelled IN (0, 1)),
    cancellation_code VARCHAR(1),
    diverted SMALLINT NOT NULL CHECK (diverted IN (0, 1)),

    crs_elapsed_time NUMERIC(10,2),
    actual_elapsed_time NUMERIC(10,2),
    air_time NUMERIC(10,2),
    distance NUMERIC(10,2) NOT NULL CHECK (distance >= 0),

    carrier_delay INTEGER NOT NULL CHECK (carrier_delay >= 0),
    weather_delay INTEGER NOT NULL CHECK (weather_delay >= 0),
    nas_delay INTEGER NOT NULL CHECK (nas_delay >= 0),
    security_delay INTEGER NOT NULL CHECK (security_delay >= 0),
    late_aircraft_delay INTEGER NOT NULL CHECK (late_aircraft_delay >= 0),

    CHECK (cancellation_code IS NULL OR cancellation_code IN ('A', 'B', 'C', 'D')),
    CHECK (dep_time IS NULL OR dep_time BETWEEN 0 AND 2400),
    CHECK (wheels_off IS NULL OR wheels_off BETWEEN 0 AND 2400),
    CHECK (wheels_on IS NULL OR wheels_on BETWEEN 0 AND 2400),
    CHECK (arr_time IS NULL OR arr_time BETWEEN 0 AND 2400)
);

COMMIT;
