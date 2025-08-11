

WITH invoice_payments AS
(
SELECT
  format_date('%Y-%m', date(invoice_date)) as invoice_month,
  gross_profit_usd,
  invoice_date,
  net_payment_days,
  DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), QUARTER), WEEK) + 1 as week_of_qtr,
  DATE_DIFF(
  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), DATE_TRUNC(CURRENT_DATE(), QUARTER),
  WEEK
) as weeks_in_qtr,
  CASE
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    THEN TRUE
    ELSE FALSE
  END as gp_last_24_hours,
  CASE
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    THEN TRUE
    ELSE FALSE
  END as gp_last_7_days,
  CASE
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    THEN TRUE
    ELSE FALSE
  END as gp_last_1_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.gsheets_stg__sales_accruals`
)
SELECT
    invoice_month,
    gross_profit_usd,
    invoice_date,
    invoice_date as Date, -- Alias 'Date' for invoice_date
    net_payment_days,
    week_of_qtr,
    weeks_in_qtr,
    gp_last_24_hours,
    gp_last_7_days,
    gp_last_1_month,
    CAST(NULL AS STRING) as function, -- Explicitly cast NULL to expected string type
    CAST(NULL AS STRING) as dept_name,
    CAST(NULL AS STRING) as metric_name,
    CAST(NULL AS STRING) as segment,
    CAST(NULL AS STRING) as target_type,
    CAST(NULL AS STRING) as currency,
    gross_profit_usd as Target, -- Alias 'Target' for gross_profit_usd
    'actuals' as data_class,
    CAST(NULL AS STRING) as target_metric
FROM invoice_payments

UNION ALL

SELECT
    CAST(NULL AS STRING) as invoice_month,
    CAST(NULL AS NUMERIC) as gross_profit_usd, -- Assuming NUMERIC or FLOAT
    CAST(NULL AS DATE) as invoice_date, -- Assuming DATE type
    Date, -- This 'Date' comes from the targets table
    -- *** ADD THESE CALCULATIONS HERE ***
    CAST(NULL AS STRING) as net_payment_days,
    DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), QUARTER), WEEK) + 1 as week_of_qtr,
    DATE_DIFF(
        DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), DATE_TRUNC(CURRENT_DATE(), QUARTER),
        WEEK
    ) as weeks_in_qtr,
    -- ***********************************
    CAST(NULL AS BOOLEAN) as gp_last_24_hours, -- Assuming BOOLEAN type
    CAST(NULL AS BOOLEAN) as gp_last_7_days,
    CAST(NULL AS BOOLEAN) as gp_last_1_month,
    function,
    dept_name,
    metric_name,
    segment,
    target_type,
    currency,
    Target, -- This 'Target' comes from the targets table
    'targets' as data_class,
    'gross_revenue' as target_metric
FROM `bi-staging-1-309112.wowzi_dbt_prod.gross_revenue_scorecard_targets`