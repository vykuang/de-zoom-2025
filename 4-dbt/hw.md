# dbt and nytaxi

## setup

- green 2019, 2020
- yellow 2019, 2020
- fhv 2019

load directly from [bq public datasets](https://console.cloud.google.com/marketplace/product/city-of-new-york/nyc-tlc-trips). The following snippet circumvents the region lock.

```SQL
-- in bq query
EXPORT DATA
  OPTIONS (
    uri = 'gs://de_zoom_2025/nytaxi/raw/green/*.parquet',
    format = 'parquet'
  ) AS (
    SELECT *
    FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019
    UNION ALL
    SELECT * 
    FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020
  )
EXPORT DATA
  OPTIONS (
    uri = 'gs://de_zoom_2025/nytaxi/raw/yellow/2019-*.parquet',
    format = 'parquet'
  ) AS (
    SELECT *
    FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019
  )
-- separate to use year specific wildcards
EXPORT DATA
  OPTIONS (
    uri = 'gs://de_zoom_2025/nytaxi/raw/yellow/2020-*.parquet',
    format = 'parquet',
    overwrite = true -- OTW throws error since dest is not empty; but will not overwrite 2019-* since names are different
  ) AS (
    SELECT * 
    FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020
  )
-- must be run in a sep. query since queries are region locked
-- creates managed, regular tables that take up space in BQ
LOAD DATA OVERWRITE zoomcamp_nytaxi.green_tripdata
FROM FILES (
  format = 'parquet',
  uris = ['gs://de_zoom_2025/nytaxi/raw/green/*.parquet']
);

-- must be run in a sep. query since queries are region locked
LOAD DATA OVERWRITE zoomcamp_nytaxi.yellow_tripdata
FROM FILES (
  format = 'parquet',
  uris = ['gs://de_zoom_2025/nytaxi/raw/yellow/*.parquet']
);
```

for `fhv`, use `web_gcs.py` from week 3 to load parquets into gcs

Alternatively to create external tables so that storage is decoupled to GCS:

```SQL
DROP TABLE IF EXISTS `de-zoom-376014.zoomcamp_nytaxi.fhv_tripdata`;
CREATE OR REPLACE EXTERNAL TABLE `de-zoom-376014.zoomcamp_nytaxi.fhv_tripdata`
    OPTIONS (
        format ="parquet",
        uris = ['gs://de_zoom_2025/nytaxi/raw/fhv/*.parquet']
    );
```

## 1 dbt model resolution

- db: myproject
- dataset: raw_ny_tripdata (default)
- table: ext_green_taxi
- fully qualified name: myproject.raw_ny_tripdata.ext_green_taxi

## 2 variable precedence

`WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

- var,
- then env_var,
- then default of env_var

## 3 lineage and exe

- `+model` - model and ancestors
- `model+` - model and descendants
- `n+model` - model and n-th degree ancestors

## 4 macros and jinja

```jinja
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

- `DBT_BIGQUERY_TARGET_DATASET` and `DBT_BIGQUERY_STAGING_DATASET` are mandatory
- core -> target
- stg/staging/anything other than core -> staging

## 5 year over year quarterly totals

```sql
with a as (
    select 
        year,
        year-1 lastyear,
        quarter,
        service_type,
        revenue_quarterly_total_amount total
    from {{ ref('fact_taxi_trips_quarterly_revenue') }}
)
select 
    a.service_type,
    a.year,
    a.quarter,
    a.total,
    b.revenue_quarterly_total_amount last_total,
    (a.total - b.revenue_quarterly_total_amount) / b.revenue_quarterly_total_amount yoy
from a
join {{ ref('fact_taxi_trips_quarterly_revenue') }} b
on a.lastyear = b.year and a.quarter = b.quarter and a.service_type = b.service_type
where a.year = 2020
order by service_type, yoy
```

- green best/worst: 1/2
- yellow best/worst: 1/2

## 6 fare percentile

green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

## 7 top nth travel time location for fhv

- locationid maps to zones
- boroughs have multiple zones

```sql
with a as (select 
    distinct pickup_zone, dropoff_zone, p90_duration,
    row_number() over(partition by pickup_zone order by p90_duration desc) rn 
from {{ ref('fact_fhv_monthly_zone_traveltime_p90') }}
where year = 2019 and month = 11
and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East'))
select pickup_zone, dropoff_zone, p90_duration
from a
where rn < 4
order by pickup_zone, p90_duration desc
```

lga, chinatown, garment