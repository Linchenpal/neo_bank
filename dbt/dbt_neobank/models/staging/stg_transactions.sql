SELECT
    transaction_id,
    transactions_type AS transaction_type,
    transactions_currency AS transaction_currency,
    amount_usd,
    transactions_state AS transaction_state,
    ea_cardholderpresence,
    ea_merchant_country,
    direction,
    user_id,
    created_at
FROM {{ source('neobank_data_raw', 'raw_transactions') }}
