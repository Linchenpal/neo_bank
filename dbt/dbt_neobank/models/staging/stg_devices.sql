SELECT
  user_id
  ,device_type
  ,{{ dbt_utils.generate_surrogate_key(['user_id', 'device_type']) }} AS device_id
FROM {{ source('raw', 'devices') }}
