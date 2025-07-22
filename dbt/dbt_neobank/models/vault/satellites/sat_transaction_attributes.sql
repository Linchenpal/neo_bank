{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS transaction_hub_id,
    transaction_type,
    transaction_currency,
    amount_usd,
    transaction_state,
    ea_cardholderpresence,
    ea_merchant_country,
    direction,
    created_at,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_transactions') }}
