{{ config(tags=["finance"]) }}

WITH successful_payments as (
    SELECT
        updated_at,
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
        date_trunc(date(updated_at), WEEK) AS payment_date_week,
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