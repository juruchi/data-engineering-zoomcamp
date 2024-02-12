-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.green_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage_zoomcamp_juruchi/green_2022_taxi/8629d88dbfff48d59e366d7ec882489f-0.parquet']
);

-- Create a table in BQ
CREATE OR REPLACE TABLE `ny_taxi.green_2022_materialized`
AS SELECT * FROM `ny_taxi.green_2022`;

-- Count number of rows in the external table
SELECT count(*) FROM `ny_taxi.green_2022`;

-- Count the distinct number of PULocationIDs in the External Table
SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi.green_2022`;

-- Count the distinct number of PULocationIDs in the Table
SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi.green_2022_materialized`;

-- Records that have a fare_amount of 0?
SELECT count(*) 
FROM `ny_taxi.green_2022_materialized`
WHERE fare_amount = 0;

-- Partition by lpep_pickup_datetime  Cluster on PUlocationID
CREATE OR REPLACE TABLE ny_taxi.green_2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `ny_taxi.green_2022`;

-- Retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

  -- Materialized table with no partitioning and no clustering
  SELECT COUNT(DISTINCT(PULocationID)) as locations
  FROM ny_taxi.green_2022_materialized
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

  -- Materialized table with partitioning and clustering
  SELECT COUNT(DISTINCT(PULocationID)) as locations
  FROM ny_taxi.green_2022_partitoned_clustered
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';