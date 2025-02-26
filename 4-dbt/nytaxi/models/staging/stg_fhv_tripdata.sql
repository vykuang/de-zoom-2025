{{
    config(
        materialized='view'
    )
}}

-- with tripdata as 
-- (
--   select *,
--     row_number() over(partition by pickup_datetime, dropOff_datetime, PUlocationID, DOlocationID) as rn
--   from {{ source('raw','fhv_tripdata') }}
--   where pickup_datetime is not null 
-- )
select
-- identifiers
    -- {{ dbt_utils.generate_surrogate_key(["pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID"]) }} as tripid,    
    dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    SR_Flag AS sr_flag,
    
    -- payment info
from {{ source('raw','fhv_tripdata') }}
-- where rn = 1
-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}