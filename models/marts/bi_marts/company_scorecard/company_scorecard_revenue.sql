WITH invoice_payments AS
(
SELECT 
  format_date('%Y-%m', date(invoice_date)) invoice_month,
  gross_profit_usd,
  invoice_date,
  CASE 
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    THEN TRUE 
    ELSE FALSE 
  END gp_last_24_hours,
  CASE 
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    THEN TRUE 
    ELSE FALSE 
  END gp_last_7_days,
  CASE 
    WHEN date(invoice_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    THEN TRUE 
    ELSE FALSE 
  END gp_last_1_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.gsheets_stg__sales_accruals` 
)

SELECT
    invoice_month,
    gross_profit_usd,
    invoice_date,
    invoice_date Date,
    DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%B %e %Y', Date), WEEK) + 1 week_of_qtr,
    gp_last_24_hours,
    gp_last_7_days,
    gp_last_1_month,
    NULL function,
    NULL dept_name,
    NULL metric_name,
    NULL segment,
    NULL target_type,
    NULL currency,
    gross_profit_usd Target,
    'actuals' data_class,
    NULL target_metric
FROM invoice_payments
UNION ALL 
SELECT 
    NULL invoice_month,
    NULL gross_profit_usd,
    NULL invoice_date,
    Date,
    NULL gp_last_24_hours,
    NULL gp_last_7_days,
    NULL gp_last_1_month,
    function,
    dept_name,
    metric_name,
    segment,
    target_type,
    currency,
    Target,
    'targets' data_class,
    'gross_revenue' target_metric
FROM `bi-staging-1-309112.wowzi_dbt_prod.gross_revenue_scorecard_targets`