-- Topic: DE Zoomcamp 3.1.1 - Data Warehouse and BigQuery
-- Referenced from below:
-- https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query.sql
-- https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25 

-- Querying public available table

-- query 1
SELECT * FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 1000


-- query 2
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

--query 3
-- create an external table with the ny_taxi data 
-- ensure that the dataset was already ingested at GCP Cloud Storage and identify its URI

CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);


-- explore the external table
-- yello trip data
SELECT * FROM `taxi-rides-ny.nytaxi.external_yellow_tripdata` limit 10;

-- FOR COMPARISON
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned` AS
SELECT * FROM `taxi-rides-ny.nytaxi.external_yellow_tripdata`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `taxi-rides-ny.nytaxi.external_yellow_tripdata`;

-- Error msg due to data type issue of some variables
-- Solution shared by Padilha (code repo below:)
-- https://github.com/padilha/de-zoomcamp/tree/master/week3 

-- non partitioned
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_non_partitioned` AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`;

-- partitioned
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`

-- Comparing the performance impact of partitioning and non-partitioning data
SELECT DISTINCT(VendorID)
FROM `taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(PULocationID)
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM `taxi-rides-ny.nytaxi.yellow_tripdata_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(PULocationID)
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-06-01' AND '2021-06-30';

-- large difference could be seen in terms of the processing and billing

SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- creating cluster data
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`; 

-- comparing performance
SELECT count(*) as trips
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2021-10-31'
AND PULocationID = 132;


SELECT count(*) as trips
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2021-10-31'
AND PULocationID = 132;

