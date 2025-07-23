with device_with_user as (
  select
    d.device_id
    ,u.user_id
    ,md5(cast(u.user_id as string)) AS user_hk
    ,md5(cast(d.device_id as string)) AS device_hk
  from {{ ref('stg_devices') }} d
  join {{ ref('stg_users') }} u
    on d.user_id = u.user_id
)

select
  md5(concat(user_hk, device_hk)) AS user_device_hk
  ,user_hk
  ,device_hk
  ,timestamp('{{ var("load_date") }}') AS load_date
  ,{{ record_source('stg_devices') }} AS record_source
from device_with_user
