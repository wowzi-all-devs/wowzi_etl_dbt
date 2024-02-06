
WITH max_starters AS 
(SELECT 
  max(job_id) + 1000 as job_id_start,
  max(campaign_id) + 1000 as campaign_id_start,
  max(company_id) + 1000 as company_id_start,
  max(task_id) + 1000 as task_id_start,
  max(influencer_id) + 1000 as influencer_id_start
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` ),

inf_id_setup AS 
(select
  influencer,
  country,
  row_number() over(order by country,influencer) as id
from
(SELECT 
  distinct
  influencer,
  country
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
where influencer is not null)
order by country,influencer),

final_influencer_ids as
(Select 
    i.influencer,
    i.country,
    m.influencer_id_start + i.id as influencer_id
from inf_id_setup i 
left join max_starters m on m.influencer_id_start = m.influencer_id_start),

campaign_id_setup AS 
(select
  campaign_name,
  country,
  row_number() over(order by campaign_name, country) as id
from
(SELECT 
  distinct
  campaign_name,
  country,
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
where influencer is not null) a
order by campaign_name, country),

final_campaign_ids as
(Select 
    i.campaign_name,
    i.country,
    m.campaign_id_start + i.id as campaign_id
from campaign_id_setup i 
left join max_starters m on m.campaign_id_start = m.campaign_id_start),

job_tasks_id_setup as 
(SELECT 
  country,
  campaign_name,
  influencer,
  row_number() over() as id
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
where influencer is not null),

final_job_tasks_ids as
(Select 
    i.campaign_name,
    i.country,
    i.influencer,
    m.job_id_start + i.id as job_id,
    m.task_id_start + i.id as task_id,
    m.company_id_start as company_id
from job_tasks_id_setup i 
left join max_starters m on m.job_id_start = m.job_id_start),

first_dates as 
(SELECT 
  country,
  influencer,
  min(payment_date) as first_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
where influencer is not null
group by country,influencer)

select 
  p.payment_date,
  p.country,
  p.agency,
  p.client,
  p.brand,
  c.campaign_id,
  p.campaign_name,
  p.temp_camp_name,
  p.social_media_platform,
  f.influencer_id,
  fd.first_date as inf_date_account_created,
  p.influencer,
  cast(p.age as int) as age,
  p.gender,
  p.influencer_level,
  j.company_id,
  j.job_id,
  j.task_id,
  p.tasks_assigned,
  p.payment_mode,
  p.bank,
  p.currency,
  p.amount_lcy,
  p.amount_usd,
  p.payment_status
from `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` p
left join final_campaign_ids c on p.campaign_name = c.campaign_name
left join final_influencer_ids f on p.influencer = f.influencer
left join final_job_tasks_ids j on p.country = j.country 
and p.campaign_name = j.campaign_name
and p.influencer = j.influencer
left join first_dates fd on p.country = fd.country
and p.influencer = fd.influencer
where p.influencer is not null