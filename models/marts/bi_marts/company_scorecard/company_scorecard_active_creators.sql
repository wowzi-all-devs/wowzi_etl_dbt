SELECT 
  DISTINCT
  DATE(task_creation_time) as Date,
  influencer_id,
  country,
  CASE 
    WHEN DATE(task_creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    THEN TRUE 
    ELSE FALSE 
  END active_last_24_hours,
  CASE 
    WHEN DATE(task_creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    THEN TRUE 
    ELSE FALSE 
  END active_last_7_days,
  CASE 
    WHEN DATE(task_creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    THEN TRUE 
    ELSE FALSE 
  END active_last_1_month,
  CAST(NULL AS STRING) function,
  CAST(NULL AS STRING)  dept_name,
  CAST(NULL AS STRING)  metric_name,
  CAST(NULL AS STRING) target_type,
  NULL target,
  'actuals' data_class
FROM {{ ref('influencer_task_facts') }}
UNION ALL      
SELECT 
  DATE(period) as Date,
  NULL influencer_id,
  NULL country,
  NULL active_last_24_hours,
  NULL active_last_7_days,
  NULL active_last_1_month,
  function,
  dept_name,
  metric_name,
  target_type,
  target,
  'targets' data_class
FROM `bi-staging-1-309112.wowzi_dbt_prod.active_creators_scorecard_targets`