SELECT
    id,
    created,
    merchant_id,
    campaign_id,
    amount,
    currency,
    ref_x,
    transfer_status,
    flw_ref,
    campaign_name
FROM {{ source('core', 'merchant_payments') }}