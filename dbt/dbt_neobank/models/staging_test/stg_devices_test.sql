select
    user_id,
    device_type
from {{ source('raw_test', 'devices') }}
