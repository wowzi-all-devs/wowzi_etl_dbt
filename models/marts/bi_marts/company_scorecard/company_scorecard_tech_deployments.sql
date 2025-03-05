SELECT 
  feature_name,
  feature_type,
  status,
  actual_release_date,
  actual_release_date period,
  'Tech' function,
  'Engineering' dept_name,
  'Deployment frequency' metric_name,
  NULL feature_type_target,
  NULL target_type,
  CASE 
    WHEN actual_release_date is not null then 1
    ELSE null 
  END target,
  'actuals' data_class
FROM {{ ref('gsheets_stg__tech_feature_deployments_data') }}
UNION ALL 
SELECT 
  NULL feature_name,
  NULL feature_type,
  NULL status,
  NULL actual_release_date,
  period,
  function,
  dept_name,
  metric_name,
  feature_type feature_type_target,
  target_type,
  target,
  'targets' data_class
FROM `bi-staging-1-309112.wowzi_dbt_prod.tech_deployment_scorecard_targets`