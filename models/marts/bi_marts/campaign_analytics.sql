WITH platform_campaigns_a AS 
(
SELECT 
    cf.campaign_id, 
    cf.campaign_name,
    cf.company_id, 
    cf.company_name,
    campaign_creation_date,
    start_date, end_date,
    currency, budget, budget_spent,
    creator_type, creator_id,
    (CASE WHEN creator_type = 'MERCHANT' then a.first_name||' '||last_name
    WHEN creator_type = 'BACK_OFFICER' then bo.backofficer_name
    else NULL
    END) AS creator_name,
    (CASE WHEN creator_type = 'MERCHANT' then a.email
    WHEN creator_type = 'BACK_OFFICER' then bo.email
    else NULL
    END) AS creator_email,
    canceled, total_tasks, quality_verified_tasks, time_verified_tasks,
    cf.advertiser_id, c.Country country
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts` cf 
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.back_officers` bo
ON cf.creator_type = bo.role AND cf.creator_id = bo.backofficer_id
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.dim_advertisers` a
ON cf.creator_id = a.advertiser_id
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.country_key` c 
ON lower(cf.country) = lower(c.Key)
  where lower(cf.company_name) not like '%demo%'
  and lower(cf.company_name) not like '%test%'
  and lower(cf.campaign_name) not like '%test%'
  and a.email not like '%kelvinm+%'
  and a.first_name||' '||last_name <> 'Eddie Adinah'
),

campaigns_requiring_manual_metrics AS 
(
  SELECT  
    distinct 
    campaign_id,
    true requires_manual_metrics
  FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` 
    where lower(channel) <> 'twitter'

),

campaigns_with_manual_metrics AS 
(
SELECT  
  campaign_id,
  true as manual_metrics  
FROM `bi-staging-1-309112.wowzi_dbt_prod.agg_manual_metric_campaign_channel` 
  group by campaign_id
  having sum(total_views) > 0
),

twitter_metrics AS 
(SELECT 
  distinct
  campaign_id,
  true has_twitter_metrics
FROM `bi-staging-1-309112.wowzi_dbt_prod.twitter_campaign_metrics`),

content_approval AS 
(
select 
  campaign_id,
  case when advertiser_skip_pre_approval is true then false 
  else true end has_content_pre_approval
FROM 
(SELECT 
  distinct 
  campaign_id,
  advertiser_skip_pre_approval
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts` )),

platform_campaigns_b AS 
(
SELECT 
    pa.campaign_id, 
    pa.campaign_name,
    pa.company_id, 
    pa.company_name,
    pa.campaign_creation_date,
    pa.start_date, 
    pa.end_date,
    pa.currency, 
    pa.budget, 
    pa.budget_spent,
    INITCAP(pa.creator_type) creator_type, 
    pa.creator_id,
    pa.creator_name,
    pa.creator_email,
    pa.canceled, 
    pa.total_tasks, 
    pa.quality_verified_tasks, 
    pa.time_verified_tasks,
    pa.advertiser_id, 
    pa.country,
    (CASE WHEN cm.requires_manual_metrics is null then false
    ELSE cm.requires_manual_metrics 
    END) AS requires_manual_metrics,
    (CASE WHEN mm.manual_metrics is null then false
    ELSE mm.manual_metrics 
    END) AS has_manual_metrics,
    (CASE WHEN tm.has_twitter_metrics is null then false
    ELSE tm.has_twitter_metrics 
    END) AS has_twitter_metrics,
    (CASE WHEN cp.has_content_pre_approval is null then false
    ELSE cp.has_content_pre_approval 
    END) AS has_content_pre_approval,
    'Platform' as datasource
FROM platform_campaigns_a pa 
LEFT JOIN campaigns_requiring_manual_metrics cm 
ON pa.campaign_id = cm.campaign_id
LEFT JOIN campaigns_with_manual_metrics mm 
ON pa.campaign_id = mm.campaign_id
LEFT JOIN twitter_metrics tm 
ON pa.campaign_id = tm.campaign_id
LEFT JOIN content_approval cp 
ON pa.campaign_id = cp.campaign_id
),

periphery_campaigns AS 
(
SELECT 
    pc.campaign_id, 
    pc.campaign_name,
    pc.company_id, 
    null company_name,
    pc.campaign_date campaign_creation_date,
    pc.campaign_date start_date, 
    pc.campaign_date end_date,
    currency, 
    null budget, 
    null budget_spent,
    null creator_type, 
    null creator_id,
    null creator_name,
    null creator_email,
    false canceled, 
    null total_tasks, 
    null quality_verified_tasks, 
    null time_verified_tasks,
    null advertiser_id, 
    pc.country,
    false requires_manual_metrics,
    false has_manual_metrics,
    false has_twitter_metrics,
    false has_content_pre_approval,
    'Periphery' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` pc
),

combined_analytics AS 
(SELECT 
    a.campaign_id, 
    a.campaign_name,
    a.company_id, 
    cast(a.company_name as string) company_name,
    a.campaign_creation_date,
    a.start_date, 
    a.end_date,
    a.currency, 
    a.budget, 
    a.budget_spent,
    cast(a.creator_type as string) creator_type, 
    a.creator_id,
    cast(a.creator_name as string) creator_name,
    cast(a.creator_email as string) creator_email,
    a.canceled, 
    a.total_tasks, 
    a.quality_verified_tasks, 
    a.time_verified_tasks,
    a.advertiser_id, 
    a.country,
    a.requires_manual_metrics,
    a.has_manual_metrics,
    a.has_twitter_metrics,
    a.has_content_pre_approval,
    a.datasource
FROM platform_campaigns_b a
UNION ALL 
SELECT 
    b.campaign_id, 
    b.campaign_name,
    b.company_id, 
    cast(b.company_name as string) company_name,
    b.campaign_creation_date,
    b.start_date, 
    b.end_date,
    b.currency, 
    b.budget, 
    b.budget_spent,
    cast(b.creator_type as string) creator_type, 
    b.creator_id,
    cast(b.creator_name as string) creator_name,
    cast(b.creator_email as string) creator_email,
    b.canceled, 
    b.total_tasks, 
    b.quality_verified_tasks, 
    b.time_verified_tasks,
    b.advertiser_id, 
    b.country,
    b.requires_manual_metrics,
    b.has_manual_metrics,
    b.has_twitter_metrics,
    b.has_content_pre_approval,
    b.datasource
FROM periphery_campaigns b 
)

SELECT 
    campaign_id, 
    campaign_name,
    company_id, 
    company_name,
    campaign_creation_date,
    start_date, 
    concat(FORMAT_DATETIME("%b", DATETIME(date(start_date))),"-", extract(year from date(start_date))) mon_yr,
    dense_rank () over (order by extract(year from start_date) asc, extract(month from start_date)asc ) mon_yr_rnk,
    end_date,
    currency, 
    budget, 
    budget_spent,
    creator_type, 
    creator_id,
    creator_name,
    creator_email,
    canceled, 
    total_tasks, 
    quality_verified_tasks, 
    time_verified_tasks,
    advertiser_id, 
    country,
    requires_manual_metrics,
    has_manual_metrics,
    has_twitter_metrics,
    has_content_pre_approval,
    datasource
FROM combined_analytics