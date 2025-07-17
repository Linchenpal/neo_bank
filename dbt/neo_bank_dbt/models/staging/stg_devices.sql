{{ config(materialized='view') }}

with source as (
    select * from {{ source('neobank_data_raw', 'raw_devices') }}
),
renamed as (
    select
        user_id,
        device_type
    from source
)

select *
from renamed
