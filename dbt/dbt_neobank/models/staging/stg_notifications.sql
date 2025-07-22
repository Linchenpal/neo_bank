SELECT
    reason,
    channel,
    status,
    user_id,
    created_at
FROM {{ source('neobank_data_raw', 'raw_notifications') }}
