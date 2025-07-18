-- models/vault/sat_user.sql

WITH source AS (
    SELECT * FROM {{ ref('stg_users') }}
),

satellite AS (
    SELECT
        {{ dbt_utils.surrogate_key(['user_id']) }} AS user_hk,
        {{ dbt_utils.surrogate_key([
            'birth_year',
            'country',
            'plan',
            'user_settings_crypto_unlocked',
            'marketing_push',
            'marketing_email',
            'num_contacts'
        ]) }} AS hashdiff,
        birth_year,
        country,
        plan,
        user_settings_crypto_unlocked,
        marketing_push,
        marketing_email,
        num_contacts,
        created_at AS load_date,
        {{ invocation_id }} AS dbt_batch_id
    FROM source
)

SELECT * FROM satellite
