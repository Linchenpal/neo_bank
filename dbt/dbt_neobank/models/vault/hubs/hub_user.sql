{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_hub_id,
    user_id,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_users') }}
