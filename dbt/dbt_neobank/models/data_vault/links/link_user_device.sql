with device_with_user as (
    select
        d.device_id,
        u.user_id,
        md5(cast(u.user_id as string)) as user_hk,
        md5(cast(d.device_id as string)) as device_hk
    from {{ ref('stg_devices') }} as d
    inner join {{ ref('stg_users') }} as u
        on d.user_id = u.user_id
)

select
    md5(concat(user_hk, device_hk)) as user_device_hk,
    user_hk,
    device_hk,
    timestamp('{{ var("load_date") }}') as load_date
    ,{{ record_source('stg_devices') }} as record_source
from device_with_user
