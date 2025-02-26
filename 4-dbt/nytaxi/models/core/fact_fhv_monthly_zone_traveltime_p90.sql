{{ config(materialized='table') }}
with t as (
    select
        year, month, pickup_locationid, dropoff_locationid,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) trip_duration
    from {{ ref('fact_fhv_trips') }}
)
select distinct
    year,
    month,
    p.zone pickup_zone,
    d.zone dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.9) OVER(partition by year, month, pickup_locationid, dropoff_locationid) p90_duration
from t
join {{ ref('dim_zones') }} p
on t.pickup_locationid = p.locationid
join {{ ref('dim_zones') }} d
on t.dropoff_locationid = d.locationid

