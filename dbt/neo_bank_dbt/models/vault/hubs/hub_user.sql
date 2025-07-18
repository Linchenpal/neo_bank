-- models/vault/hub_user.sql

WITH source AS (
    SELECT * FROM {{ ref('stg_users') }}
),

hub AS (
    SELECT
        {{ dbt_utils.surrogate_key(['user_id']) }} AS user_hk,
        user_id AS user_id,
        created_at AS load_date,
        {{ invocation_id }} AS dbt_batch_id
    FROM source
)

SELECT * FROM hub
