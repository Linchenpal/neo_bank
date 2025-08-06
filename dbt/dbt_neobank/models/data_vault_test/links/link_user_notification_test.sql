with notifications_with_user as (
    select
        n.notification_id,
        u.user_id,
        md5(cast(u.user_id as string)) as user_hk,
        md5(cast(n.notification_id as string)) as notification_hk
    from {{ ref('stg_notifications_test') }} as n
    inner join {{ ref('stg_users_test') }} as u
        on n.user_id = u.user_id
)

select
    md5(concat(user_hk, notification_hk)) as user_notification_hk,
    user_hk,
    notification_hk
    ,{{ load_date() }} as load_date
    ,{{ record_source('stg_notifications_test') }} as record_source
from notifications_with_user
