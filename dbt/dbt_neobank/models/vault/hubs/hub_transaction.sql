{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS transaction_hub_id,
    transaction_id,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_transactions') }}
