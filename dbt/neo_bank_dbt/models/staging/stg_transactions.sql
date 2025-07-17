{{ config(materialized='view') }}

with source as (
    select * from {{ source('neobank_data_raw', 'raw_transactions') }}
    limit 1000 # Limit added for testing
),
renamed as (
    select
        transaction_id,
        transactions_type,
        transactions_currency,
        cast(amount_usd as numeric) as amount_usd,
        transactions_state,
        ea_cardholderpresence,
        ea_merchant_country,
        direction,
        user_id,
        timestamp_micros(created_date) as created_at
    from source
)

select *
from renamed
