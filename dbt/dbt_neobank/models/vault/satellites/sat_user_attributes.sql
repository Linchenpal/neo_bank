{{ config(
    materialized='table',
    schema='neobank_data_vault'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_hub_id,
    birth_year,
    country,
    created_at,
    plan,
    crypto_unlocked,
    marketing_push,
    marketing_email,
    num_contacts,
    CURRENT_TIMESTAMP() AS load_date
FROM {{ ref('stg_users') }}
