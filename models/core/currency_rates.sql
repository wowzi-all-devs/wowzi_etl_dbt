WITH unique_currency_by_date AS (
SELECT
    date,
    base,
    currency,
    currency_rate,
    ROW_NUMBER() OVER(
        PARTITION BY date, base, currency
        ORDER BY date desc
    ) as row_number,
FROM {{ source('rates', 'dim_currency_rates') }}
)
SELECT
    date,
    base,
    currency,
    currency_rate
FROM 
    unique_currency_by_date
WHERE 
    row_number = 1