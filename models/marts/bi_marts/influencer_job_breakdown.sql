WITH job_details AS 
(select
  j.job_id,
  j.campaign_id,
  j.influencer_id,
  date(j.offer_creation_time) as job_offer_date,
  INITCAP(a.first_name) inf_first_name,
  INITCAP(a.last_name) inf_last_name,
  date(a.date_account_created) inf_created,
  date(a.last_campaign_date) as inf_last_campaign_date,
  a.first_campaign_date,
  INITCAP(a.gender) inf_gender,
  date(a.dob) inf_dob,
  a.age inf_age,
  a.age_range inf_age_range,
  c.Country inf_Country,
  initcap(j.invitation_status) invitation_status,
  initcap(a.influencer_type_INSTAGRAM) Instagram_influencer_level, 
  initcap(a.influencer_type_FACEBOOK) Facebook_influencer_level, initcap(a.influencer_type_TWITTER) X_influencer_level, 
  initcap(a.influencer_type_LINKEDIN) Linkedin_influencer_level, initcap(a.influencer_type_TIKTOK) Tiktok_influencer_level,
  null as influencer_level,
  null as task_id,
  null as tasks_assigned,
  null as company_id,
  null as channel,
  null amount_lcy,
  null as amount_usd,
  null as payment_date,
  null as periphery_payment_status,
  'Platform' as datasource
from `bi-staging-1-309112.wowzi_dbt_prod.job_facts` j
left join `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` a on j.influencer_id = a.influencer_id
left join `bi-staging-1-309112.wowzi_dbt_prod.country_key` c on (a.country = c.Key)
  where j.invitation_status <> 'EXPIRED'
union all 
select
  p.job_id,
  p.campaign_id,
  p.influencer_id,
  date(p.payment_date) as job_offer_date,
  INITCAP(p.influencer) inf_first_name,
  null inf_last_name,
  p.inf_date_account_created inf_created,
  p.last_campaign_date as inf_last_campaign_date,
  p.first_campaign_date as first_campaign_date,
  INITCAP(p.gender) inf_gender,
  null inf_dob,
  p.age inf_age,
  (case when p.age is null then 'No DOB'
  when p.age >= 18 and p.age <= 25 then '18-25'
  when p.age >= 26 and p.age <= 35 then '26-35'
  when p.age >= 36 and p.age <= 45 then '36-45'
  else '>45'
  end) as inf_age_range,
  INITCAP(p.country) inf_Country,
  'ACCEPTED' invitation_status,
  null Instagram_influencer_level, 
  null Facebook_influencer_level, 
  null X_influencer_level, 
  null Linkedin_influencer_level, 
  null Tiktok_influencer_level,
  p.influencer_level,
  p.task_id,
  p.tasks_assigned,
  p.company_id,
  p.social_media_platform as channel,
  p.amount_lcy as amount_lcy,
  p.amount_usd as amount_usd,
  p.payment_date,
  p.payment_status periphery_payment_status,
  'Periphery Sheet' as datasource
from `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` p
),

task_details AS
(select
    a.job_id,
    a.campaign_id,
    a.influencer_id,
    a.inf_gender gender,
    a.inf_dob dob,
    a.inf_age,
    a.inf_age_range age_groups,
    a.inf_Country country,
    a.inf_created created,
    a.job_offer_date,
    a.inf_last_campaign_date,
    a.first_campaign_date,
    DATE_DIFF(date(a.first_campaign_date), date(a.inf_created), day) as days_to_job,
    DATE_DIFF(date(current_date), date(a.inf_last_campaign_date), day) as days_since_last_campaign,
    (case when a.task_id is null then b.task_id
    else a.task_id end) as task_id,
    a.tasks_assigned,
    b.payment_amount_list,
    e.currency campaign_currency,
    case 
      when a.channel is null then b.channel
      else a.channel 
    end as channel,
    case 
      when date(b.task_creation_time) is null then a.payment_date
      else date(b.task_creation_time) 
    end task_creation_date,
    case 
      when b.third_verification_status = 'APPROVED' then 1 
      when b.third_verification_status is null then 1 
      else 0 
    end as completed_tasks,
    case 
      when c.company_industry is null then 'N/A' 
      else initcap(c.company_industry) 
    end as industry,
    case 
      when a.datasource = 'Periphery Sheet' then cast(a.amount_usd as numeric)
      else null
    end periphery_job_value_usd,
    case 
      when a.datasource = 'Platform' then cast(b.payment_amount_list as numeric)
      else null
    end platfrom_job_value_lcy,
    case 
      when a.datasource = 'Periphery Sheet' and lower(a.periphery_payment_status) = 'successful'
      then a.amount_usd
      when a.datasource = 'Platform' then d.amount_usd
    end amount_usd,
    d.payment_date,
    a.periphery_payment_status,
    Instagram_influencer_level,
    X_influencer_level,
    Linkedin_influencer_level,
    Facebook_influencer_level,
    Tiktok_influencer_level,
    invitation_status,
    influencer_level,
    a.datasource
from job_details a 
left join bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts b on (a.job_id = b.job_id)
left join bi-staging-1-309112.wowzi_dbt_prod.companies c on (b.company_id = c.id)
left join `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` d on (b.task_id = d.task_id)
and lower(d.payment_status) in ('successful', 'manual', 'new', 'completed')
left join `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` e on (cast(a.campaign_id as string) = cast(e.campaign_id as string))),

first_jobs as
(
select 
    distinct(b.influencer_id), min(first_campaign_date) first_job_date
from task_details b
group by b.influencer_id
),

task_details_with_first_job_date as
(
select 
  b.*, 
  c.first_job_date
from task_details  b
left join first_jobs c
on b.influencer_id = c.influencer_id
),

creator_refugee_status as 
(
select 
  d.*, 
  r.id_card_type
from task_details_with_first_job_date d 
left join 
`bi-staging-1-309112.wowzi_airbyte.influencer_smileidentity_data` r 
on d.influencer_id = r.influencer_id
and r.id_card_type = 'UG_TRAVEL_DOC'
),

tasks_with_refugee_status as
(select
  e.*,
  case 
    when e.tasks_assigned is null then 1 
    else cast(e.tasks_assigned as int) 
  end no_of_tasks,
  case 
    when (id_card_type is not null) then 'Refugee' 
    else 'Non-refugee' 
  end refugee_flag,
  concat(FORMAT_DATETIME("%b", DATETIME(date(job_offer_date))),"-", extract(year from date(job_offer_date))) mon_yr,
  case 
    when days_since_last_campaign <= 90 then influencer_id 
    else null 
  end active_l3m,
  -- (case when days_since_last_campaign <= 90 then 'active' else 'inactive' end) as active_l3m
  case 
    when (days_since_last_campaign <= 90) and (completed_tasks = 1) then influencer_id 
    else null 
  end active_l3m_completed_tasks,
  case 
    when (days_since_last_campaign <= 90) and (completed_tasks = 1) then amount_usd 
    else null 
  end amount_paid_to_active_l3m,
  date_diff(first_job_date, created, DAY) creation_to_job_days
  -- row_number() over (partition by influencer_id, campaign_id) rnk,/*indicate the start of a diff campaign with 1*/
  -- row_number() over (partition by influencer_id order by job_offer_date) rnk_frst_offer /*indicate the first time an offer was extended*/
from creator_refugee_status e ),

table1 as
( select 
    job_id,
    campaign_id,
    influencer_id,
    gender,
    dob,
    inf_age,
    age_groups,
    country,
    refugee_flag,
    created,
    job_offer_date,
    creation_to_job_days,
    inf_last_campaign_date,
    days_to_job,
    days_since_last_campaign,
    task_id,
    no_of_tasks,
    payment_amount_list,
    platfrom_job_value_lcy,
    campaign_currency,
    i.currency_rate,
    case 
      when datasource = 'Periphery Sheet' then periphery_job_value_usd
      when datasource = 'Platform' then platfrom_job_value_lcy/i.currency_rate
      else 0 
    end payment_amount_list_usd,
    case 
      when channel is null then 'Channel Not Set'
      else channel 
    end as channel,
    task_creation_date,
    completed_tasks,
    industry,
    case 
    when Influencer_level is not null then Influencer_level
    when 
    channel = 'FACEBOOK' and Influencer_level is null then Facebook_influencer_level
    when
    channel = 'INSTAGRAM' and Influencer_level is null then Instagram_influencer_level
    when
    channel = 'TWITTER' and Influencer_level is null then X_influencer_level
    when
    channel = 'LINKEDIN' and Influencer_level is null then Linkedin_influencer_level
    when
    channel = 'TIKTOK' and Influencer_level is null then Tiktok_influencer_level 
    else 'Uncategorized' end as Influencer_level,
    invitation_status,
    datasource,
    payment_date,
    amount_usd,
    active_l3m,
    active_l3m_completed_tasks,
    amount_paid_to_active_l3m,
    mon_yr,
    dense_rank () over (order by extract(year from job_offer_date) asc, extract(month from job_offer_date)asc) mon_yr_rnk
from tasks_with_refugee_status a 
left join `bi-staging-1-309112.wowzi_dbt_prod.int_currency_rates` i 
on (date(a.task_creation_date) = date(i.date))
and (lower(a.campaign_currency)=lower(i.currency)))

select
    a.job_id,
    a.campaign_id,
    a.influencer_id,
    a.gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.refugee_flag,
    a.created,
    a.job_offer_date,
    a.creation_to_job_days,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    a.task_id,
    a.no_of_tasks,
    payment_amount_list_usd job_value_usd,
    --a.payment_amount_list,
    --a.campaign_currency,
    --a.currency_rate,
    a.channel,
    a.task_creation_date,
    a.completed_tasks,
    a.industry,
    case 
      when a.Influencer_level is null then 'MACRO'
      else a.Influencer_level
    end Influencer_level,
    a.invitation_status,
    a.datasource,
    a.payment_date,
    a.amount_usd,
    a.active_l3m,
    a.active_l3m_completed_tasks,
    a.amount_paid_to_active_l3m,
    a.mon_yr,
    a.mon_yr_rnk,
    case 
      when (influencer_level is null) or (influencer_level = 'None') 
      then 'Uncategorized' else influencer_level
    end as influencer_level_new,
    b.company_id, 
    initcap(b.company_name) company_name,
    initcap(c.role) role, 
    initcap(c.company_role) company_role,
    /*distinct influencers month on month*/
  row_number() over(partition by a.influencer_id, a.mon_yr order by a.task_id desc) distinct_inf
from table1 a
left join `bi-staging-1-309112.wowzi_dbt_prod.campaign_facts` b
on a.campaign_id = b.campaign_id
left join bi-staging-1-309112.wowzi_dbt_prod.dim_advertisers c
on b.company_id = c.company_id