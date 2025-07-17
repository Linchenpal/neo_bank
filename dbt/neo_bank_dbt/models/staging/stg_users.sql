{{ config(materialized='view') }}

with source as (
    select * from {{ source('neobank_data_raw', 'raw_users') }}
),
renamed as (
    select
        user_id,
        birth_year,
        country,
        timestamp_micros(created_date) as created_at,
        plan,
        user_settings_crypto_unlocked,
        attributes_notifications_marketing_push as marketing_push,
        attributes_notifications_marketing_email as marketing_email,
        num_contacts
    from source
)

select *
from renamed
