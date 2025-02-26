# Data warehouse and bigquery

## gcloud sdk

- running gcloud-cli on docker is choppy when it comes to `gsutil`
- "OAuth 2.0 cred error"
- need to use `gcloud storage ...`
- `bq` seems fine

## basics

- cost: $5/1TB (flat) or $2k/month (on-demand)
- OLAP
- separation of compute vs storage
- built-in ML, geospatial, BI features
- external table - stored elsewhere, e.g. cloud storage
- partitioning narrows down scope of data to be processed

## partitioning and clustering

### partitioning

- max 4000
- time or integer
- only one column may be used for partitioning
- not beneficial on tables where frequent updates force re-partitioning

### clustering

- can improve query speed and reduce cost; queries on a clustered table scans only the relevant blocks 
- provides higher granularity
- columns to colocate related data
- order matters
- improves filter and agg
- good for larger > 1 GB datasets
- <= 4 clustering columns
- numerc, date, bool, geo, datetimes
- bq automatically reclusters

## homework

load yellow taxi trips 2024-1 to 2024-6 data into a bucket

```sh
bq mkdef --autodetect=true --source_format=PARQUET \
  gs://de_zoom_2025/*.parquet > bq_def

bq mk --table \
  --external_table_definition=bq_def \
  zoomcamp_nytaxi.yellow_taxi_ext 
```
or via sql in bq query:

```sql
CREATE EXTERNAL TABLE `PROJECT_ID.DATASET.EXTERNAL_TABLE_NAME`
  OPTIONS (
    format ="TABLE_FORMAT",
    uris = ['gs://de_zoom_2025/*.parquet'[,...]]
    );
```

Then, to [create a regular/materialized table from external](https://cloud.google.com/bigquery/docs/tables#create_a_table_that_references_an_external_data_source):

```sql
CREATE MATERIALIZED VIEW PROJECT_ID.DATASET.MATERIALIZED_VIEW_NAME AS (
  QUERY_EXPRESSION
);
```

Optimize cost for high-compute queries with small results. Use cases:

- OLAP
- ETL
- BI pipelines
- in general: pre-agg, pre-filter, pre-join, new type of clustering

However materialized view may only reference native or biglake icebergs so we fallback to a regular table

```sql
CREATE TABLE `zoomcamp_nytaxi.yellow_taxi_nonpart` AS (
  SELECT * FROM `zoomcamp_nytaxi.yellow_taxi_ext`
)
```

1. row count - 20M
    ```Sql
    SELECT count(*) FROM `de-zoom-376014.zoomcamp_nytaxi.yellow_taxi_ext`
    ```
1. count distinct PULocationID - amount of data processed between ext and nonpart? 0 and 155MB
1. selecting more columns increase data processed since BQ is columnar based
1. fare = 0 trips?
    ```sql
    select count(*)
    from `zoomcamp_nytaxi.yellow_taxi_ext`
    where fare_amount = 0
    ```
1. cluster/partition strat? partition by datetime to optimize filtering and cluster by vendorID to pre-sort
    ```sql
    CREATE OR REPLACE TABLE zoomcamp_nytaxi.yellow_taxi_part
    PARTITION BY DATE(tpep_dropoff_datetime) 
    CLUSTER BY VendorID AS 
    SELECT * FROM zoomcamp_nytaxi.yellow_taxi_ext
    ```
1. after partitioning and clustering, filtering costs 26 MB; nonpartitioned table costs 310 MB
1. ext is stored on GCS
1. it is not always best to cluster a table
    - overhead of clustering means effect is only realized on larger >1GB datasets
    - only useful for columns that are queried in tandem
    - cardinality is an important metric - if too high
1. count(*) is a cached metadata and thus cost 0 B; if a date filter was added, data will be processed


### external table from GCS

```SQL
DROP TABLE IF EXISTS `de-zoom-376014.zoomcamp_nytaxi.fhv_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-376014.zoomcamp_nytaxi.fhv_tripdata`
  OPTIONS (
    format ="parquet",
    uris = ['gs://de_zoom_2025/nytaxi/raw/fhv/*.parquet']
    );

    DROP TABLE IF EXISTS `de-zoom-376014.zoomcamp_nytaxi.yellow_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-376014.zoomcamp_nytaxi.yellow_tripdata`
  OPTIONS (
    format ="parquet",
    uris = ['gs://de_zoom_2025/nytaxi/raw/yellow/*.parquet']
    );
    DROP TABLE IF EXISTS `de-zoom-376014.zoomcamp_nytaxi.green_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-376014.zoomcamp_nytaxi.green_tripdata`
  OPTIONS (
    format ="parquet",
    uris = ['gs://de_zoom_2025/nytaxi/raw/green/*.parquet']
    );