WITH max_starters AS 
(
SELECT 
  max(job_id) + 1000 AS job_id_start,
  max(campaign_id) + 1000 AS campaign_id_start,
  max(company_id) + 1000 AS company_id_start,
  max(task_id) + 1000 AS task_id_start,
  max(influencer_id) + 1000 AS influencer_id_start
FROM {{ ref('influencer_task_facts') }} 
),

inf_id_setup AS 
(
SELECT
  influencer,
  country,
  row_number() OVER(ORDER BY country,influencer) AS id
FROM
(
SELECT 
  DISTINCT
  influencer_name influencer,
  country
FROM {{ source('staging', 'periphery_markets_data') }} 
)
  ORDER BY country,influencer
),

final_influencer_ids AS
(
SELECT 
    i.influencer,
    i.country,
    m.influencer_id_start + i.id AS influencer_id
FROM inf_id_setup i 
LEFT JOIN max_starters m ON m.influencer_id_start = m.influencer_id_start
),

campaign_id_setup AS 
(
SELECT
  campaign_name,
  country,
  row_number() OVER(ORDER BY campaign_name, country) AS id
FROM
(
SELECT 
  DISTINCT
  campaign_name,
  country,
FROM {{ source('staging', 'periphery_markets_data') }} 
) a
  ORDER BY campaign_name, country
),

final_campaign_ids AS
(
SELECT 
    i.campaign_name,
    i.country,
    m.campaign_id_start + i.id AS campaign_id
FROM campaign_id_setup i 
LEFT JOIN max_starters m ON m.campaign_id_start = m.campaign_id_start
),

job_tasks_id_setup AS 
(
SELECT 
  country,
  campaign_name,
  influencer_name influencer,
  row_number() OVER() AS id
FROM {{ source('staging', 'periphery_markets_data') }} 
),

final_job_tasks_ids AS
(
SELECT 
    i.campaign_name,
    i.country,
    i.influencer,
    m.job_id_start + i.id AS job_id,
    m.task_id_start + i.id AS task_id,
    m.company_id_start AS company_id
FROM job_tasks_id_setup i 
LEFT JOIN max_starters m ON m.job_id_start = m.job_id_start
),

first_dates AS 
(
SELECT 
  country,
  influencer_name influencer,
  min(payment_date) AS first_date
FROM {{ source('staging', 'periphery_markets_data') }} 
  GROUP BY country,influencer_name
),

last_dates AS 
(
SELECT 
  country,
  influencer_name influencer,
  max(payment_date) AS last_date
FROM {{ source('staging', 'periphery_markets_data') }}
  GROUP BY country,influencer_name
),

final_output AS
(
SELECT 
  p.campaign_date,
  p.payment_date,
  p.country,
  p.agency,
  p.client,
  p.brand,
  c.campaign_id,
  p.campaign_name,
  NULL temp_camp_name,
  CASE 
    WHEN p.social_media_platform IS NULL THEN 'INSTAGRAM'
    ELSE upper(p.social_media_platform) 
  END social_media_platform,
  f.influencer_id,
  fd.first_date inf_date_account_created,
  fd.first_date first_campaign_date,
  ld.last_date last_campaign_date,
  p.influencer_name influencer,
  case 
    when p.age is null then 26
    else cast(p.age as int) 
  end age,
  case 
    when p.age is null then '26-35'
    when cast(p.age as int) >= 18 and cast(p.age as int) <= 25 then '18-25'
    when cast(p.age as int) >= 26 and cast(p.age as int) <= 35 then '26-35'
    when cast(p.age as int) >= 36 and cast(p.age as int) <= 45 then '36-45'
    else '>45' 
  end as inf_age_range,
  case 
    when p.gender is null then 'Male'
    else p.gender 
  end gender,
  case when p.influencer_level is null then 'Macro'
  else p.influencer_level end influencer_level,
  j.company_id,
  j.job_id,
  j.task_id,
  p.tasks_assigned,
  p.payment_mode,
  p.bank,
  p.currency,
  COALESCE(p.amount_lcy, 0.0) amount_lcy,
  COALESCE(p.amount_usd, 0.0) amount_usd,
  p.payment_status
from {{ source('staging', 'periphery_markets_data') }} p
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
)

SELECT
  campaign_date,
  payment_date,
  country,
  agency,
  client,
  brand,
  campaign_id,
  campaign_name,
  temp_camp_name,
  social_media_platform,
  influencer_id,
  inf_date_account_created,
  first_campaign_date,
  last_campaign_date,
  influencer,
  age,
  inf_age_range,
  gender,
  influencer_level,
  company_id,
  job_id,
  task_id,
  tasks_assigned,
  payment_mode,
  bank,
  currency,
  amount_lcy,
  (CASE 
    WHEN amount_usd = 0 and lower(country) = 'angola'
    THEN amount_lcy*0.0012
    WHEN amount_usd = 0 and lower(country) = 'botswana'
    THEN amount_lcy*0.073
    WHEN amount_usd = 0 and lower(country) = 'ethiopia'
    THEN amount_lcy*0.017
    WHEN amount_usd = 0 and lower(country) = 'kenya'
    THEN amount_lcy*0.0077
    WHEN amount_usd = 0 and lower(country) = 'mauritius'
    THEN amount_lcy*0.022
    WHEN amount_usd = 0 and lower(country) = 'mozambique'
    THEN amount_lcy*0.016
    WHEN amount_usd = 0 and lower(country) = 'namibia'
    THEN amount_lcy*0.054
    WHEN amount_usd = 0 and lower(country) = 'south africa'
    THEN amount_lcy*0.054
    WHEN amount_usd = 0 and lower(country) = 'tanzania'
    THEN amount_lcy*0.00038
    WHEN amount_usd = 0 and lower(country) = 'tanzania'
    THEN amount_lcy*0.00026
    WHEN amount_usd = 0 and lower(country) = 'zambia'
    THEN amount_lcy*0.037
    WHEN amount_usd = 0 and lower(country) = 'zimbabwe'
    THEN amount_lcy*1
    ELSE amount_usd
  END) amount_usd,
  payment_status
FROM final_output