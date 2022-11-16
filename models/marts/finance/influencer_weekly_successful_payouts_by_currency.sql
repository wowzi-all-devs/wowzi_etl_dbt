{{ config(tags=["finance"]) }}

WITH successful_payments as (
    SELECT
        payment_date,
        currency,
        amount,
        payment_status
    FROM 
        {{ ref('influencer_payouts') }}
    WHERE
        payment_status NOT IN ('New', 'FAILED')
),
successful_payments_by_week_by_currency as ( 
    SELECT
        TIMESTAMP_TRUNC(TIMESTAMP(SAFE_CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E3SZ', payment_date) as DATETIME)), WEEK) AS payment_date_week,
        currency,
        sum(amount) AS total_amount
    FROM 
        successful_payments
    GROUP BY 
        currency,
        payment_date_week
    ORDER BY 
        total_amount DESC
)
SELECT *
FROM successful_payments_by_week_by_currency