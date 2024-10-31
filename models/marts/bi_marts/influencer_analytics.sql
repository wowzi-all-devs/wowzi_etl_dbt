WITH inf_facts AS
(
SELECT 
  influencer_id,
  date_account_created,
  first_name||' '||last_name influencer_name,
  age,
  gender,
  c.Country country,
  location,
  income_category,
  income_currency,
  smileidentity_status,
  job_eligibility,
  job_activity,
  username_INSTAGRAM,
  influencer_type_INSTAGRAM,
  channel_status_INSTAGRAM,
  username_FACEBOOK,
  influencer_type_FACEBOOK,
  channel_status_FACEBOOK,
  username_TWITTER,
  influencer_type_TWITTER,
  channel_status_TWITTER,
  username_TIKTOK,
  influencer_type_TIKTOK,
  channel_status_TIKTOK,
  username_LINKEDIN,
  influencer_type_LINKEDIN,
  channel_status_LINKEDIN,
  first_campaign_date,
  last_campaign_date
FROM bi-staging-1-309112.wowzi_dbt_prod.influencer_facts f 
LEFT join bi-staging-1-309112.wowzi_dbt_prod.country_key c ON f.country = c.Key
where 
(lower(email) not like '%@getnada.com%'
AND lower(email) not like '%wowzi%'
AND lower(email) not like '%@fivermail.com%'
AND lower(email) not like '%@g.com%'
AND lower(email) not like '%@example.com%'
AND lower(email) not like '%@getairmail.com%') OR email IS NULL
),

inf_tasks AS 
(
SELECT 
  jf.influencer_id,
  jf.campaign_id,
  jf.job_id,
  jf.invitation_status,
  tf.task_id,
  date(tf.task_creation_time) task_createdat,
  case 
      when tf.third_verification_status = 'APPROVED' then 'Complete'
      when tf.third_verification_status = 'NOT_VERIFIED'
      and date(cf.end_date) > date(current_date)
      then 'Ongoing'
      else 'Failed' 
  end task_status,
  tf.company_id,
  cf.company_name,
  cf.currency,
  tf.channel,
  tf.submission_status,
  tf.payment_amount_list,
  p.amount_usd paid_amount_usd,
  (CASE
    WHEN p.amount_usd IS NULL THEN 'Not Paid'
    ELSE 'Paid'
  END) task_payment_status,
  tf.country
FROM `bi-staging-1-309112.wowzi_dbt_prod.job_facts` jf 
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts`tf ON jf.influencer_id = tf.influencer_id
AND jf.job_id = tf.job_id
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts` cf ON jf.campaign_id = cf.campaign_id
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` p on (tf.task_id = p.task_id)
and lower(p.payment_status) in ('successful', 'manual', 'new', 'completed')
order by jf.influencer_id, jf.job_id
),

inf_tasks2 AS
(
SELECT 
    it.influencer_id,
    it.campaign_id,
    it.job_id,
    it.invitation_status,
    it.task_id,
    it.task_createdat,
    it.company_id,
    (CASE
        WHEN it.company_name IS NULL THEN e.company_name
        ELSE it.company_name
    END) company_name,
    it.currency,
    it.channel,
    it.submission_status,
    it.payment_amount_list,
    i.currency_rate,
    COALESCE(it.payment_amount_list/i.currency_rate, 0.0) payment_amount_list_usd,
    it.paid_amount_usd,
    it.task_payment_status,
    it.country
FROM inf_tasks it
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.int_currency_rates` i 
on (date(it.task_createdat) = date(i.date))
and (lower(it.currency)=lower(i.currency))
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` e on (cast(it.campaign_id as string) = cast(e.campaign_id as string))
)


SELECT 
    f.influencer_id,
    f.date_account_created,
    INITCAP(f.influencer_name) influencer_name,
    f.age,
    f.gender,
    INITCAP(f.country) country,
    INITCAP(f.location) location,
    f.income_category,
    UPPER(f.income_currency) income_currency,
    f.smileidentity_status,
    f.job_eligibility,
    f.job_activity,
    f.username_INSTAGRAM,
    f.influencer_type_INSTAGRAM,
    f.channel_status_INSTAGRAM,
    f.username_FACEBOOK,
    f.influencer_type_FACEBOOK,
    f.channel_status_FACEBOOK,
    f.username_TWITTER,
    f.influencer_type_TWITTER,
    f.channel_status_TWITTER,
    f.username_TIKTOK,
    f.influencer_type_TIKTOK,
    f.channel_status_TIKTOK,
    f.username_LINKEDIN,
    f.influencer_type_LINKEDIN,
    f.channel_status_LINKEDIN,
    f.first_campaign_date,
    f.last_campaign_date,
    it.campaign_id,
    it.job_id,
    it.invitation_status,
    it.task_id,
    it.task_createdat,
    it.company_id,
    INITCAP(it.company_name) company_name,
    co.company_industry,
    it.currency,
    it.channel,
    it.submission_status,
    it.payment_amount_list,
    it.currency_rate,
    it.payment_amount_list_usd,
    it.paid_amount_usd,
    it.task_payment_status
    --it.country
FROM inf_facts f
LEFT JOIN inf_tasks2 it ON f.influencer_id =it.influencer_id
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.companies` co ON it.company_id = co.id