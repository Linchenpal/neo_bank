select
    user_id,
    md5(cast(user_id as string)) as user_hk
    ,{{ load_date() }} as load_date
    ,{{ record_source('stg_users_test') }} as record_source
from {{ ref('stg_users_test') }}
