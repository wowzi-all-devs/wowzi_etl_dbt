WITH payments AS 
(
SELECT 
  influencer_id,
  task_id,
  payment_eligible_at,
  expected_payment_date,
  payment_date,
  country,
  currency,
  amount,
  amount_usd,
  DATE_DIFF(DATE(payment_date), DATE(expected_payment_date), DAY) AS target,
  ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY payment_date) row_num
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts`
  WHERE lower(payment_status) IN ('successful', 'manual', 'complete', 'new')
  AND DATE(payment_date) >= '2025-01-01'
)

SELECT 
  influencer_id,
  task_id,
  payment_eligible_at,
  expected_payment_date,
  payment_date,
  payment_date Date,
  country,
  currency,
  amount,
  amount_usd,
  target,
  'actuals' data_class,
  NULL target_metric,
  NULL target_type,
  NULL function,
  NULL dept_name,
  NULL metric_name
FROM payments
  WHERE row_num = 1
UNION ALL 
SELECT 
  NULL influencer_id,
  NULL task_id,
  NULL payment_eligible_at,
  NULL expected_payment_date,
  NULL payment_date,
  Date,
  NULL country,
  NULL currency,
  NULL amount,
  NULL amount_usd,
  target,
  'targets' data_class,
  'AP' target_metric,
  target_type,
  function,
  dept_name,
  metric_name  
FROM `bi-staging-1-309112.wowzi_dbt_prod.accounts_payable_scorecard_targets`