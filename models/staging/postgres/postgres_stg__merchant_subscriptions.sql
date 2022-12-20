SELECT
    id,
    merchant_id,
    subscription_id,
    merchant_email,
    status,
    date_created,
    valid_until,
    payment_plan_id
FROM {{ source('staging', 'merchant_subscriptions') }}