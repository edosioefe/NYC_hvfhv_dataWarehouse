-- COPY COMMAND
copy hvfhv_fact
from 's3://efenycprocessed/'
iam_role 'arn:aws:iam::654654314882:role/redshift_read_s3'
region 'eu-west-1'
JOB CREATE hvfhv_copy_job
AUTO ON;

copy hvfhv_fact
from 's3://efenycprocessed/fhvhv_tripdata_2023-01.parquet'
iam_role 'arn:aws:iam::654654314882:role/redshift_read_s3'
FORMAT AS PARQUET;



select * from core_dw."Time_dim"
CREATE TABLE date_dim(
	time_id BIGINT PRIMARY KEY,
	time DATE
)

select * from core_dw."Date_dim"
CREATE TABLE date_dim(
	date_id BIGINT PRIMARY KEY,
	date DATE
)


select * from core_dw."Location_dim" 
CREATE TABLE location_dim(
	location_id INT PRIMARY KEY,
	borough VARCHAR,
	"zone" VARCHAR,
	service_zone VARCHAR
)



select * from core_dw."Shared_ride_dim"
CREATE TABLE sr_dim(
	shared_ride_id INT PRIMARY KEY,
	request VARCHAR,
	"match" VARCHAR
)


select * from core_dw."HVFHV_license_dim"
CREATE TABLE company_dim(
	company_id VARCHAR PRIMARY KEY,
	company_name VARCHAR
)


CREATE TABLE ride(
	request_date_id BIGINT,
	on_scene_date_id BIGINT,
	pickup_date_id BIGINT,
	dropoff_date_id BIGINT,
	request_time_id BIGINT,
	on_scene_time_id BIGINT,
	pickup_time_id BIGINT,
	dropoff_time_id BIGINT,
	pu_location_id INT,
	do_location_id INT,
	company_id VARCHAR,
	shared_ride_id INT,	
	trip_miles NUMERIC,
	trip_time INT,
	base_passenger_fare NUMERIC,
	extra_charges NUMERIC,
	tips NUMERIC

)


----------VIEWS
CREATE VIEW vw_request_date_dim AS 
	SELECT date_id as request_date_id, "date" as request_date
	FROM date_dim

CREATE VIEW vw_on_scene_date_dim AS
	SELECT date_id as on_scene_date_id, "date" as on_scene_date
	FROM date_dim

CREATE VIEW vw_pickup_date_dim AS
	SELECT date_id as pickup_date_id, "date" as pickup_date
	FROM date_dim

CREATE VIEW vw_dropoff_date_dim AS
	SELECT date_id as dropoff_date_id, "date" as dropoff_date
	FROM date_dim
------
CREATE VIEW vw_request_time_dim AS 
	SELECT time_id as request_time_id, "time" as request_time
	FROM time_dim

CREATE VIEW vw_on_scene_time_dim AS
	SELECT time_id as on_scene_time_id, "time" as on_scene_time
	FROM time_dim

CREATE VIEW vw_pickup_time_dim AS
	SELECT time_id as pickup_time_id, "time" as pickup_time
	FROM time_dim

CREATE VIEW vw_dropoff_time_dim AS
	SELECT time_id as dropoff_time_id, "time" as dropoff_time
	FROM time_dim





























