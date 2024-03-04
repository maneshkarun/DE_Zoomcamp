{{
    config(
        materialized='view'
    )
}}

with source as (
   select *
   from {{ source('staging', 'fhv_tripdata') }}
)

select * from source