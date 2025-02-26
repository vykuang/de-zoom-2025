{{ config(materialized='table') }}

with fhv as (
    select
        *,
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    extract(year from f.pickup_datetime) year,
    extract(month from f.pickup_datetime) month,
    f.*,
    pu.borough as pickup_borough,
    pu.zone as pickup_zone,
    d.borough as dropoff_borough,
    d.zone as dropoff_zone
from fhv f
join dim_zones pu
on f.pickup_locationid = pu.locationid
join dim_zones d
on f.dropoff_locationid = d.locationid