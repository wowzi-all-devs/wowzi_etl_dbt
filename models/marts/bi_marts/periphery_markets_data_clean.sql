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
  influencer_name influencer,
  country
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` )
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
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` ) a
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
  influencer_name influencer,
  row_number() over() as id
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` ),

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
  influencer_name influencer,
  min(payment_date) as first_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
group by country,influencer_name),

last_dates as 
(SELECT 
  country,
  influencer_name influencer,
  max(payment_date) as last_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data` 
group by country,influencer_name)

select 
  p.payment_date,
  p.country,
  p.agency,
  p.client,
  p.brand,
  c.campaign_id,
  p.campaign_name,
  p.temp_camp_name,
  upper(p.social_media_platform) social_media_platform,
  f.influencer_id,
  fd.first_date inf_date_account_created,
  fd.first_date first_campaign_date,
  ld.last_date last_campaign_date,
  p.influencer_name influencer,
  case when p.age is null then 26
  else cast(p.age as int) end age,
  case when p.age is null then '26-35'
  when cast(p.age as int) >= 18 and cast(p.age as int) <= 25 then '18-25'
  when cast(p.age as int) >= 26 and cast(p.age as int) <= 35 then '26-35'
  when cast(p.age as int) >= 36 and cast(p.age as int) <= 45 then '36-45'
  else '>45' end as inf_age_range,
  case when p.gender is null then 'Male'
  else p.gender end gender,
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
and p.country = c.country
left join final_influencer_ids f on p.influencer_name = f.influencer
and p.country = f.country
left join final_job_tasks_ids j on p.country = j.country 
and p.campaign_name = j.campaign_name
and p.influencer_name = j.influencer
left join first_dates fd on p.country = fd.country
and p.influencer_name = fd.influencer
left join last_dates ld on p.country = ld.country
and p.influencer_name = ld.influencer
