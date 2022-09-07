SELECT
    date,
    base,
    currency,
    currency_rate
FROM {{ source('rates', 'currency_rates_test') }}