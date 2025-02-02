WITH cohort_data AS 
(
SELECT 
  cohort_month,
  cohort_mon,
  cohort_yr,
  total_inf,
  month_number,
  active_month,
  active_inf,
  retention_rate
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_retention_rates` 
),

three_month_retention AS (
SELECT
    c1.active_month,
    AVG(c2.retention_rate) AS avg_3_month_retention_rate
FROM cohort_data c1
LEFT JOIN cohort_data c2 ON
c2.cohort_month BETWEEN DATE_SUB(c1.active_month, INTERVAL 3 MONTH) AND c1.active_month
    GROUP BY c1.active_month
)

SELECT
  active_month period,
  NULL function,
  NULL dept_name,
  NULL metric_name,
  NULL target_type,
  avg_3_month_retention_rate target,
  'actuals' data_class
FROM three_month_retention
UNION ALL 
SELECT 
  period,
  function,
  dept_name,
  metric_name,
  target_type,
  target,
  'targets' data_class
FROM `bi-staging-1-309112.wowzi_dbt_prod.monthly_creator_retention_scorecard_targets` 