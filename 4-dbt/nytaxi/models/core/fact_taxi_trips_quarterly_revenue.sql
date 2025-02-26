{{ config(materialized='table') }}
select 
    -- Revenue grouping 
    year,
    quarter, 
    service_type, 
    -- pickup_zone as revenue_zone,
    -- Revenue calculation 
    sum(fare_amount) as revenue_quarterly_fare,
    sum(extra) as revenue_quarterly_extra,
    sum(mta_tax) as revenue_quarterly_mta_tax,
    sum(tip_amount) as revenue_quarterly_tip_amount,
    sum(tolls_amount) as revenue_quarterly_tolls_amount,
    sum(ehail_fee) as revenue_quarterly_ehail_fee,
    sum(improvement_surcharge) as revenue_quarterly_improvement_surcharge,
    sum(total_amount) as revenue_quarterly_total_amount,

    -- Additional calculations
    count(tripid) as total_quarterly_trips,
    avg(passenger_count) as avg_quarterly_passenger_count,
    avg(trip_distance) as avg_quarterly_trip_distance

from {{ ref('fact_trips') }}
group by 1,2,3