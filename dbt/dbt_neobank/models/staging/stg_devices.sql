SELECT
    user_id,
    device_type
FROM {{ source('neobank_data_raw', 'raw_devices') }}
