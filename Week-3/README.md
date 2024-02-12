## Creating External Table from Parquet File available in GCS
```sql
CREATE EXTERNAL TABLE `evident-time-410307.ny_taxi.external_green_taxi_2022`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://evident-time-410307-example-bucket/nyc_green_taxi_data_2022/e8fe47b4334f4b7cbe1861685d03d05d-0.parquet']
  );
```
## Creating Materialized Table from External Table
```sql
CREATE OR REPLACE TABLE `evident-time-410307.ny_taxi.green_taxi_2022`
AS SELECT * FROM `evident-time-410307.ny_taxi.external_green_taxi_2022`;
```
## Question 3
```sql
SELECT COUNT(DISTINCT(PULocationID)) FROM `evident-time-410307.ny_taxi.green_taxi_2022`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `evident-time-410307.ny_taxi.external_green_taxi_2022`;
```
## Question 4
```sql
SELECT COUNT(*) FROM `evident-time-410307.ny_taxi.green_taxi_2022` WHERE fare_amount = 0;
```
## Question 5
```sql
CREATE OR REPLACE TABLE evident-time-410307.ny_taxi.green_taxi_2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `evident-time-410307.ny_taxi.green_taxi_2022`;
```
## Question 6
```sql
SELECT DISTINCT PULocationID
FROM evident-time-410307.ny_taxi.green_taxi_2022_partitoned_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM evident-time-410307.ny_taxi.green_taxi_2022
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
```