-- WITH influencers AS 
-- /*inf_info*/
-- (
-- SELECT 
--   safe_cast(influencer_id as string) influencer_id_a, 
--   initcap(d.first_name) first_name, 
--   initcap(d.last_name) last_name, 
--   case when lower(d.smileidentity_status) = 'pending' then 'Not Attempted'
--   else initcap(d.smileidentity_status) end smileidentity_status, 
--   d.job_eligibility,
--   d.date_account_created,
--   e.Country country,
--   FORMAT_DATETIME("%b", DATETIME(date(date_account_created))) acc_cre_mon,
--   dense_rank () over (order by extract(year from date_account_created), extract(month from date_account_created) desc) acc_cre_rnk
-- ---from  bi-staging-1-309112.wowzi_dbt_prod.influencer_facts d
-- from {{ref('influencer_facts')}} d
-- ---left join bi-staging-1-309112.dbt_kayode.country_key e
-- left join {{ source('staging','country_key') }} e
-- on (d.country = e.Key)
-- where 
-- (lower(email) not like '%@getnada.com%'
-- AND lower(email) not like '%wowzi%'
-- AND lower(email) not like '%@fivermail.com%'
-- AND lower(email) not like '%@g.com%'
-- AND lower(email) not like '%@example.com%'
-- AND lower(email) not like '%@getairmail.com%') OR email IS NULL
-- ),
-- campaign_data AS 
-- (
-- SELECT 
--   safe_cast(b.campaign_id as string) campaign_id, 
--   b.campaign_name, 
--   safe_cast(b.company_id as string) company_id, 
--   initcap(b.company_name) company_name,
--   safe_cast(a.job_id as string) job_id, 
--   safe_cast(a.influencer_id as string) influencer_id,
--   b.currency currency,
--   date(b.start_date) campaign_start_date,
--   date(a.offer_creation_time) offer_creation_time_job_offer_date,
--   FORMAT_DATETIME("%b", DATETIME(date(a.offer_creation_time))) Job_offer_mon,
--   dense_rank () over (order by extract(year from date(a.offer_creation_time)), extract(month from date(a.offer_creation_time)) desc) Job_offer_mon_rnk,
-- ---from bi-staging-1-309112.wowzi_dbt_prod.campaign_facts b
-- from {{ ref('campaign_facts') }} b
-- ---JOIN bi-staging-1-309112.wowzi_dbt_prod.job_facts a
-- JOIN {{ ref('job_facts') }} a
-- ON 
--   (safe_cast(b.campaign_id as string) = safe_cast(a.campaign_id as string))
-- AND (a.invitation_status = 'ACCEPTED')
-- ),
-- inf_data as
-- (
-- select 
-- influencers.*,
-- campaign_data.*
-- from
-- campaign_data
-- join 
-- influencers
-- on 
-- campaign_data.influencer_id = influencers.influencer_id_a
-- ),
-- tasks as
-- (
-- select 
-- inf_data.influencer_id,
-- inf_data.influencer_id_a,
-- inf_data.first_name,
-- inf_data.last_name,
-- inf_data.smileidentity_status,
-- inf_data.country,
-- inf_data.job_eligibility,
-- inf_data.date_account_created,
-- inf_data.acc_cre_mon,
-- inf_data.acc_cre_rnk,
-- inf_data.campaign_id,
-- inf_data.campaign_name,
-- inf_data.campaign_start_date,
-- inf_data.job_id,
-- inf_data.company_name,
-- inf_data.company_id,
-- inf_data.offer_creation_time_job_offer_date,
-- inf_data.Job_offer_mon,
-- inf_data.Job_offer_mon_rnk,
-- inf_data.currency,
-- tasks.task_id,
-- tasks.third_verification_status,
-- date(tasks.task_creation_time) task_creation_time,
-- dense_rank () over (order by extract(year from date(tasks.task_creation_time)), extract(month from date(tasks.task_creation_time)) desc) task_creation_mon_rnk,
-- case when tasks.third_verification_status = 'APPROVED' then 1 else 0 end as completed_tasks
-- from
-- --- bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts tasks 
-- {{ref ('influencer_task_facts') }} tasks
-- left join
-- inf_data
-- on safe_cast(tasks.job_id as INT64) = SAFE_CAST(inf_data.job_id as INT64)
-- )
-- select 
-- * from 
-- tasks


WITH influencers AS 
/*inf_info*/
(
SELECT 
  safe_cast(influencer_id as string) influencer_id_a, 
  initcap(d.first_name) first_name, 
  initcap(d.last_name) last_name, 
  --d.email, 
  --initcap(d.gender)gender, 
  case when lower(d.smileidentity_status) = 'pending' then 'Not Attempted'
  else initcap(d.smileidentity_status) end smileidentity_status, 
  --d.age, 
  d.job_eligibility,
  d.date_account_created,
  e.Country country,
  FORMAT_DATETIME("%b", DATETIME(date(date_account_created))) acc_cre_mon,
  dense_rank () over (order by extract(year from date_account_created), extract(month from date_account_created) desc) acc_cre_rnk
from  {{ ref('influencer_facts') }} d
left join {{ source('staging','country_key') }} e
on (d.country = e.Key)
where 
(lower(email) not like '%@getnada.com%'
AND lower(email) not like '%wowzi%'
AND lower(email) not like '%@fivermail.com%'
AND lower(email) not like '%@g.com%'
AND lower(email) not like '%@example.com%'
AND lower(email) not like '%@getairmail.com%') OR email IS NULL
),
campaign_data AS 
(
SELECT 
  safe_cast(b.campaign_id as string) campaign_id, 
  b.campaign_name, 
  safe_cast(b.company_id as string) company_id, 
  initcap(b.company_name) company_name,
  safe_cast(a.job_id as string) job_id, 
  safe_cast(a.influencer_id as string) influencer_id,
  safe_cast(c.task_id as string) task_id,
  b.currency currency,
  --Initcap(a.invitation_status) invitation_status,
  DATE_DIFF(CURRENT_DATE(), date(start_date), WEEK) + 1 week_of_qtr,
  date(b.start_date) campaign_start_date,
  date(a.offer_creation_time) offer_creation_time_job_offer_date,
  --date(c.submission_link_add_time) submission_link_date_task_submission, 
  --date(c.task_creation_time) task_creation_time,
  case when c.third_verification_status = 'APPROVED' then 1 else 0 end as completed_tasks,
  FORMAT_DATETIME("%b", DATETIME(date(a.offer_creation_time))) Job_offer_mon,
  dense_rank () over (order by extract(year from date(a.offer_creation_time)), extract(month from date(a.offer_creation_time)) desc) Job_offer_mon_rnk,
  date(c.task_creation_time) task_creation_time,
  dense_rank () over (order by extract(year from date(c.task_creation_time)), extract(month from date(c.task_creation_time)) desc) task_creation_mon_rnk
  --safe_cast(b.creator_id as STRING) merchant_id, 
  --b.budget_spent, 
  --c.payment_amount_list payout, /*expected payout*/
  --initcap(c.channel) submission_channel, 
  --submission_link, 
  --date(c.submission_link_add_time) submission_link_date
FROM {{ ref('campaign_facts') }} b
JOIN {{ ref('job_facts') }} a
ON 
  (safe_cast(b.campaign_id as string) = safe_cast(a.campaign_id as string))
AND (a.invitation_status = 'ACCEPTED')
JOIN {{ ref('influencer_task_facts') }} c 
ON 
  (safe_cast(a.job_id as string) = safe_cast(c.job_id as string))
WHERE b.company_id not in 
(SELECT id 
FROM `bi-staging-1-309112.wowzi_dbt_prod.companies` 
where lower(email) like '%wowzi%')
and b.company_id not in (16035,16032,5555, 17394,17364,17395,17615,17744,18698,387)
)

SELECT 
  a.campaign_id, 
  a.campaign_name, 
  a.company_id, 
  a.company_name,
  a.job_id, 
  --a.influencer_id,
  b.influencer_id_a influencer_id,
  b.first_name,
  b.last_name,
  b.smileidentity_status, 
  b.job_eligibility,
  b.date_account_created,
  b.country,
  b.acc_cre_mon,
  b.acc_cre_rnk,
  a.task_id,
  a.currency currency,
  a.campaign_start_date,
  week_of_qtr,
  a.offer_creation_time_job_offer_date,
  a.Job_offer_mon,
  a.Job_offer_mon_rnk,
  a.task_creation_time,
  a.task_creation_mon_rnk,
  a.completed_tasks
FROM influencers b 
LEFT JOIN campaign_data a
ON (b.influencer_id_a = a.influencer_id )