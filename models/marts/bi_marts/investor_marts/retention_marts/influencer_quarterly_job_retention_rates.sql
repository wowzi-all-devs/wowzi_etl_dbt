SELECT  
  *,
  'Nano' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_nano` 
union all 
SELECT  
  *,
  'Micro' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_micro` 
union all
SELECT  
  *,
  'Macro' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_macro` 
union all
SELECT  
  *,
  'Mega' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_mega` 
union all
SELECT  
  *,
  'Meta' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_meta` 
union all
SELECT 
  *,
  'All' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_quarterly_job_retention_rates_alllevels`