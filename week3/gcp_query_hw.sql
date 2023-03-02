-- Code walkalong 
-- repo: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse 
-- sources: https://www.youtube.com/watch?v=j8r2OigKBWE&list=PL3MmuxUbc_hJjEePXIdE-LVUx_1ZZjYGW&index=8


CREATE OR REPLACE EXTERNAL TABLE `prefect-test-366216.hw.fhv_external_table`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp2/*.gz']
);

CREATE OR REPLACE TABLE `prefect-test-366216.hw.fhv_materialized_table`
as (
    SELECT * FROM `prefect-test-366216.hw.fhv_external_table`
)


-- Q1
SELECT COUNT(*) 
FROM `prefect-test-366216.hw.fhv_materialized_table`;

-- Q2
SELECT COUNT(DISTINCT(affiliated_base_number)) 
FROM `prefect-test-366216.hw.fhv_materialized_table`;

--Q3
SELECT count(*)
FROM `prefect-test-366216.hw.fhv_materialized_table`
WHERE PULocationID IS NULL and DOlocationID IS NULL;


CREATE OR REPLACE TABLE `prefect-test-366216.hw.fhv_partitioned_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `prefect-test-366216.hw.fhv_materialized_table`
);

-- Q5
SELECT DISTINCT affiliated_base_number
FROM `prefect-test-366216.hw.fhv_partitioned_table`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
