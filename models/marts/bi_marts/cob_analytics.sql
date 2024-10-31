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
from bi-staging-1-309112.wowzi_dbt_prod.influencer_facts d
left join bi-staging-1-309112.dbt_kayode.country_key e
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
  date(b.start_date) campaign_start_date,
  date(a.offer_creation_time) offer_creation_time_job_offer_date,
  --date(c.submission_link_add_time) submission_link_date_task_submission, 
  --date(c.task_creation_time) task_creation_time,
  case when c.third_verification_status = 'APPROVED' then 1 else 0 end as completed_tasks,
  FORMAT_DATETIME("%b", DATETIME(date(a.offer_creation_time))) Job_offer_mon,
  dense_rank () over (order by extract(year from date(a.offer_creation_time)), extract(month from date(a.offer_creation_time)) desc) Job_offer_mon_rnk
  --safe_cast(b.creator_id as STRING) merchant_id, 
  --b.budget_spent, 
  --c.payment_amount_list payout, /*expected payout*/
  --initcap(c.channel) submission_channel, 
  --submission_link, 
  --date(c.submission_link_add_time) submission_link_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts`b
JOIN bi-staging-1-309112.wowzi_dbt_prod.job_facts a
ON 
  (safe_cast(b.campaign_id as string) = safe_cast(a.campaign_id as string))
AND (a.invitation_status = 'ACCEPTED')
JOIN bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts c 
ON 
  (safe_cast(a.job_id as string) = safe_cast(c.job_id as string) )
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
  a.offer_creation_time_job_offer_date,
  a.Job_offer_mon,
  a.Job_offer_mon_rnk,
  a.completed_tasks
FROM influencers b 
LEFT JOIN campaign_data a
ON (b.influencer_id_a = a.influencer_id )