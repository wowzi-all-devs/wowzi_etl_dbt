SELECT 
  DISTINCT
  DATE(start_date) as Date,
  DATE_DIFF(CURRENT_DATE(), date(start_date), WEEK) + 1 week_of_qtr
  creator_id,
  country,
  CASE 
    WHEN DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    THEN TRUE 
    ELSE FALSE 
  END active_last_24_hours,
  CASE 
    WHEN DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    THEN TRUE 
    ELSE FALSE 
  END active_last_7_days,
  CASE 
    WHEN DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    THEN TRUE 
    ELSE FALSE 
  END active_last_1_month,
  CAST(NULL AS STRING) function,
  CAST(NULL AS STRING)  dept_name,
  CAST(NULL AS STRING)  metric_name,
  CAST(NULL AS STRING) target_type,
  NULL target,
  'actuals' data_class
  from bi-staging-1-309112.wowzi_dbt_prod.campaign_facts
-- FROM {{ ref('campaign_facts') }}

UNION ALL  

SELECT 
  DATE,
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
FROM bi-staging-1-309112.wowzi_dbt_prod.active_creators_updated_scorecard_targets 