
{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'transaction_id']) }} AS link_user_transaction_id,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_hub_id,
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS transaction_hub_id,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_transactions') }}
