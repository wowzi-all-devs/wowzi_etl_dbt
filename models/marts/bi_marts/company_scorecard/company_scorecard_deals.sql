SELECT 
  id,
  dealname,
  deal___select_brand_name,
  dealtype,
  dealstage,
  pipeline,
  deal_currency_code,
  hs_forecast_amount, 
  hs_closed_amount,
  deal___country,
  days_to_close,
  hs_is_closed,
  closedate,
  hs_is_closed_won,
  hs_closed_won_date,
  createdAt,
  hs_created_by_user_id,
  archived,
  hubspot_owner_id,
  hubspot_owner_assigneddate,
  'Commercial' function,
  NULL dept_name,
  NULL metric_name,
  brand_payment_segment segment,
  CASE
    WHEN hs_is_closed IS FALSE THEN DATE(createdAt)
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS TRUE THEN DATE(hs_closed_won_date)
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS FALSE THEN DATE(closedate)
  END Date,
  NULL target_type,
  NULL currency,
  CASE
    WHEN hs_is_closed IS FALSE THEN hs_forecast_amount
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS TRUE THEN hs_closed_amount
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS FALSE THEN hs_forecast_amount
  END target,
  CASE
    WHEN hs_is_closed IS FALSE THEN 'actuals_pipeline'
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS TRUE THEN 'actuals_deals'
    WHEN hs_is_closed IS TRUE AND hs_is_closed_won IS FALSE THEN 'deals_lost'
  END data_class,
  NULL target_metric
FROM `bi-staging-1-309112.wowzi_dbt_prod.hubspot_deals_facts`
WHERE pipeline = '202561015'
AND id NOT IN ('10007253435','12916878270','30231459055')
UNION ALL 
SELECT  
  NULL id,
  NULL dealname,
  NULL deal___select_brand_name,
  NULL dealtype,
  NULL dealstage,
  NULL pipeline,
  NULL deal_currency_code,
  NULL hs_forecast_amount, 
  NULL hs_closed_amount,
  NULL deal___country,
  NULL days_to_close,
  NULL hs_is_closed,
  NULL closedate,
  NULL hs_is_closed_won,
  NULL hs_closed_won_date,
  NULL createdAt,
  NULL hs_created_by_user_id,
  NULL archived,
  NULL hubspot_owner_id,
  NULL hubspot_owner_assigneddate,
  function,
  dept_name,
  metric_name,
  segment,
  Date,
  target_type,
  currency,
  target,
  'targets' data_class,
  'deals' target_metric
FROM `bi-staging-1-309112.wowzi_dbt_prod.deals_scorecard_targets` 
UNION ALL 
SELECT  
  NULL id,
  NULL dealname,
  NULL deal___select_brand_name,
  NULL dealtype,
  NULL dealstage,
  NULL pipeline,
  NULL deal_currency_code,
  NULL hs_forecast_amount, 
  NULL hs_closed_amount,
  NULL deal___country,
  NULL days_to_close,
  NULL hs_is_closed,
  NULL closedate,
  NULL hs_is_closed_won,
  NULL hs_closed_won_date,
  NULL createdAt,
  NULL hs_created_by_user_id,
  NULL archived,
  NULL hubspot_owner_id,
  NULL hubspot_owner_assigneddate,
  function,
  dept_name,
  metric_name,
  segment,
  Date,
  target_type,
  currency,
  target,
  'targets' data_class,
  'pipeline' target_metric
FROM `bi-staging-1-309112.wowzi_dbt_prod.pipeline_value_scorecard_targets`