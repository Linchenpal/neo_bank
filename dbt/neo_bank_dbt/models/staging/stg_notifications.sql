{{ config(materialized='view') }}

with source as (
    select * from {{ source('neobank_data_raw', 'raw_notifications') }}
),
renamed as (
    select
        reason,
        channel,
        status,
        user_id,
        timestamp_micros(created_date) as created_at
    from source
)

select *
from renamed
