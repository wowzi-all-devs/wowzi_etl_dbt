SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'Nano' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_monthly_job_retention_rates_nano` 
union all 
SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'Micro' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_monthly_job_retention_rates_micro` 
union all 
SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'Macro' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_monthly_job_retention_rates_macro` 
union all 
SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'Mega' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_monthly_job_retention_rates_mega` 
union all 
SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'Meta' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_monthly_job_retention_rates_meta` 
union all 
SELECT  
  cohort_month,
  cohort_mon_yr,
  cohort_mon,
  cohort_yr,
  cohort_yr_rnk,
  total_inf,
  month_number,
  active_inf,
  retention_rate,
  'All' as inf_level
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_retention_rates` 