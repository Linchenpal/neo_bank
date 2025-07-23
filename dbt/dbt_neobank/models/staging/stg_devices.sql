select
    user_id,
    device_type,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'device_type']) }} as device_id
from {{ source('raw', 'devices') }}
