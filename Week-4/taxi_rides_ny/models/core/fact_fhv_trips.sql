{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    SELECT * FROM {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    SELECT * FROM {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.drop_off_datetime,
    fhv_tripdata.p_ulocation_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_tripdata.d_olocation_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    pickup_zone.service_zone
 from 
    fhv_tripdata inner join dim_zones as pickup_zone 
    on fhv_tripdata.p_ulocation_id = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on fhv_tripdata.d_olocation_id = dropoff_zone.locationid
    where EXTRACT(YEAR FROM pickup_datetime) = 2019
