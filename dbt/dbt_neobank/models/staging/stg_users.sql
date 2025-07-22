SELECT
    user_id,
    birth_year,
    country,
    created_at,
    plan,
    user_settings_crypto_unlocked AS crypto_unlocked,
    attributes_notifications_marketing_push AS marketing_push,
    attributes_notifications_marketing_email AS marketing_email,
    num_contacts
FROM {{ source('neobank_data_raw', 'raw_users') }}
