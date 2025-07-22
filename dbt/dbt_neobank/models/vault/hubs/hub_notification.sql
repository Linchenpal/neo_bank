{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'channel', 'created_at']) }} AS notification_hub_id,
    user_id,
    channel,
    created_at,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_notifications') }}
