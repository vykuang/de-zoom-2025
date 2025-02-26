{{ config(materialized='table') }}
select 
    distinct service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.9) over(partition by service_type, year, month) p90,
    PERCENTILE_CONT(fare_amount, 0.95) over(partition by service_type, year, month) p95,
    PERCENTILE_CONT(fare_amount, 0.97) over(partition by service_type, year, month) p97
from {{ ref('fact_trips') }}
where fare_amount > 0 
and trip_distance > 0 
and payment_type_description in ('Cash', 'Credit Card')