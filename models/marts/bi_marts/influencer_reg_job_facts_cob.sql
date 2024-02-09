WITH inf_details as 
(select 
  safe_cast(influencer_id as string) influencer_id_a, 
  d.age_range inf_age_range,
  initcap(d.first_name) first_name, 
  initcap(d.last_name) last_name,
  d.mobile_number,
  d.email, 
  initcap(d.gender)gender,
  d.smileidentity_status, 
  d.age, d.job_eligibility,
  d.date_account_created,
  d.first_campaign_date inf_first_campaign_date,
  date(d.last_campaign_date) inf_last_campaign_date,
  e.Country country,
 concat( FORMAT_DATETIME("%b", date_account_created ),"-", extract(year from date_account_created) ) acc_cre_mon_yr,
  FORMAT_DATETIME("%b", DATETIME(date(date_account_created))) acc_cre_mon,
  extract(year from date_account_created) acc_cre_yr,
  'Platform' as datasource
from bi-staging-1-309112.wowzi_dbt_prod.influencer_facts d
left join bi-staging-1-309112.wowzi_dbt_prod.country_key e
on
(d.country = e.Key)
union all 
SELECT 
     safe_cast(p.influencer_id as string) influencer_id_a, 
     p.inf_age_range,
    initcap(p.influencer) first_name, 
    null last_name,
    null as mobile_number,
    null as email, 
    initcap(p.gender) gender,
    'APPROVED' as smileidentity_status, 
    p.age, 
    true as job_eligibility,
    p.inf_date_account_created date_account_created,
    p.first_campaign_date inf_first_campaign_date,
    p.last_campaign_date inf_last_campaign_date,
    initcap(p.Country) country,
    concat( FORMAT_DATETIME("%b", p.inf_date_account_created ),"-", extract(year from p.inf_date_account_created) ) acc_cre_mon_yr,
    FORMAT_DATETIME("%b", DATETIME(date(p.inf_date_account_created))) acc_cre_mon,
    extract(year from p.inf_date_account_created) acc_cre_yr,
    'Periphery Sheet' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` p),

inf_details_ranked as
(select 
  influencer_id_a, 
  inf_age_range,
  first_name, 
  last_name,
  mobile_number,
  left(mobile_number, 4) as dial_code,
  c.Country clean_country,
  email, 
  case when gender is null then 'Gender Not Set'
  else gender end gender,
  smileidentity_status, 
  age, 
  job_eligibility,
  date_account_created,
  inf_first_campaign_date,
  inf_last_campaign_date,
  i.country,
  acc_cre_mon_yr,
  acc_cre_mon,
  acc_cre_yr,
  dense_rank () over (order by extract(year from date_account_created) asc, extract(month from date_account_created)asc ) acc_cre_rnk,
  datasource
from inf_details i
left join bi-staging-1-309112.wowzi_dbt_prod.country_key c 
on cast(left(i.mobile_number, 4) as string) = cast(c.Dial_code as string)),

influencer_occupations as 
(
SELECT
  distinct
  safe_cast(isb.influencer_id as int) influencer_id,
  s.parent_category_id,
  c.enum_value as parent_category,
  isb.subcategory_id,
  s.enum_value
FROM `bi-staging-1-309112.wowzi_airbyte.influencer_subcategories` isb
left join `bi-staging-1-309112.wowzi_airbyte.subcategories`s on isb.subcategory_id = s.id
left join `bi-staging-1-309112.wowzi_airbyte.categories` c on s.parent_category_id = c.id
where c.enum_value = 'PROFESSION'
and isb.subcategory_type = 'OCCUPATION'
order by isb.influencer_id 
),

inf_details_ranked2 as 
(select
  i.influencer_id_a, 
  i.inf_age_range,
  i.first_name, 
  i.last_name,
  i.mobile_number,
  clean_country,
  i.email, 
  i.gender,
  i.smileidentity_status, 
  i.age, 
  i.job_eligibility,
  i.date_account_created,
  inf_first_campaign_date,
  inf_last_campaign_date,
  DATE_DIFF(date(i.inf_first_campaign_date), date(i.date_account_created), day) as days_to_job,
  i.country,
  i.acc_cre_mon_yr,
  i.acc_cre_mon,
  i.acc_cre_yr,
  i.acc_cre_rnk,
  i.datasource,
  o.parent_category_id,
  initcap(o.parent_category) parent_category,
  o.subcategory_id,
  initcap(o.enum_value) enum_value
from inf_details_ranked i 
left join influencer_occupations o 
on i.influencer_id_a = cast(o.influencer_id as string)),

tasks_details as
(select
    safe_cast(a.campaign_id as string) campaign_id, 
    safe_cast(a.job_id as string) job_id,
    safe_cast(a.influencer_id as string) influencer_id,
    safe_cast(c.task_id as string) task_id,
    1 as no_of_tasks,
     d.currency currency_mrg,
    Initcap(a.invitation_status) invitation_status,
    date(b.campaign_start_date) campaign_start_date,
    date(a.offer_creation_time) offer_creation_time_job_offer_date,
    date(c.submission_link_add_time) submission_link_date_task_submission,
    date(c.task_creation_time) task_creation_time,
    case when c.third_verification_status = 'APPROVED' then 1 else 0 end as completed_tasks,
    b.campaign_name, safe_cast(b.merchant_id as STRING) merchant_id,
    initcap(b.company_name) company_name,
    cast(b.budget_spent as string) budget_spent,
    safe_cast(c.company_id as string) company_id,
    cast(c.payment_amount_list as NUMERIC) payout,
    initcap(c.channel) submission_channel, submission_link,
    date(c.submission_link_add_time) submission_link_date,
    null as currency,
    null as amount_usd,
    null as periphery_payment_status
from bi-staging-1-309112.wowzi_dbt_prod.job_facts a
left join bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure b
on
(safe_cast(a.campaign_id as string) = safe_cast(b.campaign_id as string))
left join bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts c
on
(safe_cast(a.job_id as string) = safe_cast(c.job_id as string) )
left join bi-staging-1-309112.wowzi_dbt_staging.campaigns d
on
(safe_cast(a.campaign_id as string) = safe_cast(d.campaign_id as string))
union all 
select
    safe_cast(p.campaign_id as string) campaign_id, 
    safe_cast(p.job_id as string) job_id,
    safe_cast(p.influencer_id as string) influencer_id,
    safe_cast(p.task_id as string) task_id,
    case when tasks_assigned is null then 1
    else cast(tasks_assigned as int) end as no_of_tasks,
    p.currency currency_mrg,
    'ACCEPTED' invitation_status,
    date(p.payment_date) campaign_start_date,
    date(p.payment_date) offer_creation_time_job_offer_date,
    date(p.payment_date) submission_link_date_task_submission,
    date(p.payment_date) task_creation_time,
    1 as completed_tasks,
    p.campaign_name, 
    null merchant_id,
    p.client company_name,
    cast(p.amount_lcy as string) budget_spent,
    safe_cast(p.company_id as string) company_id,
    cast(p.amount_lcy as NUMERIC) payout,
    initcap(p.social_media_platform) submission_channel, 
    null as submission_link,
    date(p.payment_date) submission_link_date,
    p.currency,
    p.amount_usd,
    p.payment_status periphery_payment_status
from `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` p),

task_payments as 
(select 
    x.task_id, 
    x.payment_date, 
    x.amount, 
    x.currency,
    y.dte, 
    y.currency currecy_b, 
    y.currency_rate /*rate when payment was made*/,
  -- x.amount / y.currency_rate amount_usd
from bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts x
left join dbt_kayode.vw_currency_rate y on (x.currency = y.currency)
and (date(date(x.payment_date)) = date(y.dte))),

tasks_payments2 as
(select 
    b.*, 
    case when b.amount_usd > 0 then cast(b.task_id as int)
    else cast(c.task_id as int) end paid_tasks, 
    case when b.amount_usd > 0 then b.campaign_start_date
    else c.payment_date end payment_date, 
    case when b.amount_usd > 0 then b.payout
    else c.amount end amount_local, 
    b.currency_mrg local_currency, 
    c.currency_rate currency_rate_date_paid---, c.amount_usd
from tasks_details b
left join task_payments c
on safe_cast(b.task_id as string) = safe_cast(c.task_id as string)),

final_table as
(select 
  d.*,
  k.currency_rate currency_rate_task_created /*Rate when task was created*/
from tasks_payments2 d
left join
dbt_kayode.vw_currency_rate k
on
(d.local_currency = k.currency)
and
(date(d.task_creation_time) = date(k.dte))),

prep_table as
(select
  case when (a.influencer_id_a is not null) then influencer_id_a
  else e.influencer_id end as influencer_id,
  a.inf_age_range,
  a.parent_category_id,
  a.parent_category,
  a.subcategory_id,
  case when a.enum_value is null then 'Not Set'
  else a.enum_value end inf_profession,
  a.first_name, a.last_name,
  a.mobile_number,
  a.email,
  case when a.gender = 'Gender' then 'Gender Not Set'
  else a.gender end gender,
  a.smileidentity_status, a.age, a.job_eligibility,
  e.campaign_name, e.merchant_id,
  e.company_name,
  e.campaign_id, 
  e.job_id,
  e.task_id,
  e.no_of_tasks,
  e.completed_tasks,
  e.invitation_status,
  e.submission_link,
  e.campaign_start_date,
  a.date_account_created,
  a.days_to_job,
  e.offer_creation_time_job_offer_date,
  e.submission_channel,
  e.submission_link_date_task_submission,
  case when a.country is null and clean_country is not null then clean_country
  when a.country is null and clean_country is null then 'Kenya'
  else a.country end country, 
  e.budget_spent, 
  e.company_id,
  e.currency_rate_task_created,
  e.payout,
  case when e.amount_usd > 0 then e.amount_usd
  else e.payout / e.currency_rate_task_created end expected_payout_usd,
  case when (e.paid_tasks is not null) then 'paid' else 'not paid' end as paid_task,
  e.payment_date,
  e.local_currency, 
  e.currency_rate_date_paid, 
  e.amount_local,
  case 
    when datasource = 'Periphery Sheet' and e.amount_usd>0 and lower(e.periphery_payment_status) = 'successful'
    then e.amount_usd
    when datasource = 'Platform'
    then e.amount_local / e.currency_rate_task_created 
    else null 
  end paid_amount_usd,
  a.acc_cre_mon,
  a.acc_cre_yr,
  a.acc_cre_rnk,
  a.acc_cre_mon_yr,
  FORMAT_DATETIME("%b", DATETIME(date(offer_creation_time_job_offer_date))) job_offer_mon,
  extract(year from offer_creation_time_job_offer_date) job_offer_yr,
  dense_rank () over (order by extract(month from offer_creation_time_job_offer_date) desc) job_offer_mon_rnk,
  datasource
  from inf_details_ranked2 a
  full join final_table e
    on a.influencer_id_a = e.influencer_id),

gender_fill as
(select 
  b.influencer_id,
  b.rownum,
  case when b.rownum < c.num then 'Male'
  else 'Female'
  end as gender_fill
from
(select
  influencer_id,
  row_number() over(order by influencer_id) as rownum
from
(SELECT 
  distinct
  influencer_id
FROM prep_table
  where influencer_id is not null
  and gender = 'Gender Not Set') a 
  order by influencer_id) b
  left join 
  (SELECT 
  cast(count(distinct influencer_id)*0.7 as int) as num
FROM prep_table
  where influencer_id is not null
  and gender = 'Gender Not Set') c on c.num = c.num)

select 
  p.influencer_id,
  inf_age_range,
  parent_category_id,
  parent_category,
  subcategory_id,
  inf_profession,
  first_name, 
  last_name,
  mobile_number,
  email,
  case when gender is null or gender = 'Gender Not Set' then g.gender_fill
  else gender end gender,
  smileidentity_status,
  age, 
  job_eligibility,
  campaign_name, 
  merchant_id,
  company_name,
  campaign_id, 
  job_id,
  task_id,
  no_of_tasks,
  completed_tasks,
  invitation_status,
  submission_link,
  campaign_start_date,
  date_account_created,
  days_to_job,
  offer_creation_time_job_offer_date,
  submission_channel,
  submission_link_date_task_submission,
  country, 
  budget_spent, 
  company_id,
  currency_rate_task_created,
  payout,
  expected_payout_usd,
  paid_task,
  payment_date,
  local_currency, 
  currency_rate_date_paid, 
  amount_local,
  paid_amount_usd,
  acc_cre_mon,
  acc_cre_yr,
  acc_cre_rnk,
  acc_cre_mon_yr,
  job_offer_mon,
  job_offer_yr,
  job_offer_mon_rnk,
  datasource
from prep_table p
left join gender_fill g on cast(p.influencer_id as string) = cast(g.influencer_id as string)
  where p.influencer_id is not null
  