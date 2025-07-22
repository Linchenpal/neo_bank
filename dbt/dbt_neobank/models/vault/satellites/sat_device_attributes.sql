{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS device_hub_id,
    device_type,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_devices') }}
