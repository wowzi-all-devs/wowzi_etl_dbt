With platform_jobs AS  ---Platform Jobs duplicates records bc an influencer can be paid on separate days.
(
SELECT 
    t.influencer_id,
    f.email,
    t.company_id,
    e.company_name,
    case 
      when co.company_industry is null then 'N/A' 
      else initcap(co.company_industry) 
    end as industry,
    t.campaign_id,
    campaign_end_date,
    t.job_id,
    date(j.offer_creation_time) as job_offer_date,
    t.task_id,
    date(t.task_creation_time) task_creation_date,
    1 tasks_assigned,
    case 
        when t.third_verification_status = 'APPROVED' then 'Complete'
        when t.third_verification_status = 'NOT_VERIFIED'
        and date(e.campaign_end_date) > date(current_date)
        then 'Ongoing'
        else 'Failed' 
    end job_status,
    case 
        when t.third_verification_status = 'APPROVED' then 1
        when t.third_verification_status = 'NOT_VERIFIED'
        and date(e.campaign_end_date) > date(current_date)
        then 1
        else 0
    end completed_tasks,
    COALESCE(CAST(t.payment_amount_list AS FLOAT64), 0.0) amount_lcy,
    e.currency campaign_currency,
    COALESCE(CAST(d.amount_usd AS FLOAT64), 0.0) amount_usd,
    d.payment_date payment_date,
    null as periphery_payment_status,
    null as periphery_job_value_usd,
    cast(t.payment_amount_list as numeric) platfrom_job_value_lcy,
    INITCAP(f.first_name) inf_first_name,
    INITCAP(f.last_name) inf_last_name,
    date(f.date_account_created) created,
    f.first_campaign_date,
    date(f.last_campaign_date) as inf_last_campaign_date,
    DATE_DIFF(date(f.first_campaign_date), date(f.date_account_created), day) as days_to_job,
    DATE_DIFF(date(current_date), date(f.last_campaign_date), day) as days_since_last_campaign,
    INITCAP(f.gender) gender,
    date(f.dob) dob,
    f.age inf_age,
    f.age_range age_groups,
    c.Country country,
    initcap(j.invitation_status) invitation_status,
    initcap(f.influencer_type_INSTAGRAM) Instagram_influencer_level, 
    initcap(f.influencer_type_FACEBOOK) Facebook_influencer_level, 
    initcap(f.influencer_type_TWITTER) X_influencer_level, 
    initcap(f.influencer_type_LINKEDIN) Linkedin_influencer_level, 
    initcap(f.influencer_type_TIKTOK) Tiktok_influencer_level,
    t.channel,
    case 
        when channel = 'TWITTER' then initcap(f.influencer_type_TWITTER)
        when channel = 'FACEBOOK' then initcap(f.influencer_type_FACEBOOK)
        when channel = 'INSTAGRAM' then initcap(f.influencer_type_INSTAGRAM)
        when channel = 'TIKTOK' then initcap(f.influencer_type_TIKTOK)
        when channel = 'LINKEDIN' then initcap(f.influencer_type_LINKEDIN)
    end influencer_level,
  'Platform' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_task_facts` t
left join `bi-staging-1-309112.wowzi_dbt_prod.job_facts` j on t.influencer_id = j.influencer_id
and t.campaign_id = j.campaign_id and t.job_id = j.job_id
left join `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f on  t.influencer_id = f.influencer_id
left join `bi-staging-1-309112.wowzi_dbt_prod.country_key` c on (t.country = c.Key)
left join `bi-staging-1-309112.wowzi_dbt_prod.campaign_expenditure` e on (cast(t.campaign_id as string) = cast(e.campaign_id as string))
left join bi-staging-1-309112.wowzi_dbt_prod.companies co on (t.company_id = co.id)
left join `bi-staging-1-309112.wowzi_dbt_prod.influencer_payouts` d on (t.task_id = d.task_id)
and lower(d.payment_status) in ('successful', 'manual', 'new', 'completed')
),

job_details AS
(
SELECT 
    influencer_id,
    company_id,
    company_name,
    industry,
    campaign_id,
    campaign_end_date,
    job_id,
    job_offer_date,
    task_id,
    task_creation_date,
    tasks_assigned,
    job_status,
    completed_tasks,
    amount_lcy,
    campaign_currency,
    amount_usd,
    payment_date,
    CAST(periphery_payment_status AS STRING) periphery_payment_status,
    CAST(periphery_job_value_usd AS NUMERIC) periphery_job_value_usd,
    platfrom_job_value_lcy,
    inf_first_name,
    inf_last_name,
    created,
    first_campaign_date,
    inf_last_campaign_date,
    days_to_job,
    days_since_last_campaign,
    gender,
    dob,
    inf_age,
    age_groups,
    country,
    invitation_status,
    Instagram_influencer_level, 
    Facebook_influencer_level, 
    X_influencer_level, 
    Linkedin_influencer_level, 
    Tiktok_influencer_level,
    channel,
    influencer_level,
    datasource
FROM platform_jobs f
    WHERE (LOWER(f.email) NOT LIKE '%@getnada.com%'
    AND LOWER(f.email) NOT LIKE '%wowzi%'
    AND LOWER(f.email) NOT LIKE '%@fivermail.com%'
    AND LOWER(f.email) NOT LIKE '%@g.com%'
    AND LOWER(f.email) NOT LIKE '%@example.com%'
    AND LOWER(f.email) NOT LIKE '%@getairmail.com%') OR f.email IS NULL
UNION ALL 
SELECT 
    p.influencer_id,
    p.company_id,
    p.brand company_name,
    'Fmcg' industry,
    p.campaign_id,
    NULL campaign_end_date,
    p.job_id,
   DATE(p.campaign_date) AS job_offer_date,
    p.task_id,
    DATE(p.campaign_date) task_creation_date,
    CASE
        WHEN p.tasks_assigned IS NULL THEN 1
        ELSE p.tasks_assigned
    END tasks_assigned,
    CASE 
        WHEN lower(p.payment_status) = 'successful' THEN 'Complete'
        ELSE 'Ongoing'
    END job_status,
    1 AS completed_tasks,
    p.amount_lcy,
    p.currency campaign_currency,
    CASE 
      WHEN lower(p.payment_status) = 'successful'
      THEN p.amount_usd
      ELSE NULL
    END amount_usd,
    DATE(p.payment_date) payment_date,
    CAST(p.payment_status AS STRING) periphery_payment_status,
    CAST(p.amount_usd AS NUMERIC) periphery_job_value_usd,
    NULL platfrom_job_value_lcy,
    INITCAP(p.influencer) inf_first_name,
    NULL inf_last_name,
    DATE(p.inf_date_account_created) created,
    DATE(p.first_campaign_date) first_campaign_date,
    DATE(p.last_campaign_date) AS inf_last_campaign_date,
    DATE_DIFF(DATE(p.first_campaign_date), DATE(p.inf_date_account_created), DAY) AS days_to_job,
    DATE_DIFF(DATE(current_date), DATE(p.last_campaign_date), DAY) AS days_since_last_campaign,
    INITCAP(p.gender) gender,
    null as dob,
    p.age inf_age,
    CASE WHEN p.age IS NULL THEN 'No DOB'
         WHEN p.age >= 18 AND p.age <= 25 THEN '18-25'
         WHEN p.age >= 26 AND p.age <= 35 THEN '26-35'
         WHEN p.age >= 36 AND p.age <= 45 THEN '36-45'
         ELSE '>45'
    END AS age_groups,
    p.Country country,
    'ACCEPTED' invitation_status,
    NULL AS Instagram_influencer_level, 
    NULL AS Facebook_influencer_level, 
    NULL AS X_influencer_level, 
    NULL AS Linkedin_influencer_level, 
    NULL AS Tiktok_influencer_level,
    p.social_media_platform channel,
    p.influencer_level,
    'Periphery Sheet' AS datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` p),

first_jobs as
(
select 
    distinct(b.influencer_id), min(first_campaign_date) first_job_date
from job_details b
group by b.influencer_id
),

job_details_with_first_job_date as
(
select 
    a.influencer_id,
    a.company_id,
    a.industry,
    a.campaign_id,
    a.company_name,
    a.campaign_end_date,
    a.job_id,
    a.job_offer_date,
    a.task_id,
    a.task_creation_date,
    a.tasks_assigned,
    a.job_status,
    a.completed_tasks,
    a.amount_lcy,
    a.campaign_currency,
    a.amount_usd,
    a.payment_date,
    a.periphery_payment_status,
    a.periphery_job_value_usd,
    a.platfrom_job_value_lcy,
    a.inf_first_name,
    a.inf_last_name,
    a.created,
    a.first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    a.gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.invitation_status,
    a.Instagram_influencer_level, 
    a.Facebook_influencer_level, 
    a.X_influencer_level, 
    a.Linkedin_influencer_level, 
    a.Tiktok_influencer_level,
    a.channel,
    a.influencer_level,
    a.datasource, 
    b.first_job_date
from job_details  a
left join first_jobs b
on a.influencer_id = b.influencer_id
),

creator_refugee_status as 
(
select 
    a.influencer_id,
    a.company_id,
    a.industry,
    a.campaign_id,
    a.company_name,
    a.campaign_end_date,
    a.job_id,
    a.job_offer_date,
    a.task_id,
    a.task_creation_date,
    a.tasks_assigned,
    a.job_status,
    a.completed_tasks,
    a.amount_lcy,
    a.campaign_currency,
    a.amount_usd,
    a.payment_date,
    a.periphery_payment_status,
    a.periphery_job_value_usd,
    a.platfrom_job_value_lcy,
    a.inf_first_name,
    a.inf_last_name,
    a.created,
    a.first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    a.gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.invitation_status,
    a.Instagram_influencer_level, 
    a.Facebook_influencer_level, 
    a.X_influencer_level, 
    a.Linkedin_influencer_level, 
    a.Tiktok_influencer_level,
    a.channel,
    a.influencer_level,
    a.datasource, 
    a.first_job_date, 
    case
        when b.id_card_type is null and a.influencer_id in (138950,122329,123398,   120417) then 'UG_TRAVEL_DOC'
        else b.id_card_type
    end id_card_type
from job_details_with_first_job_date a
left join 
`bi-staging-1-309112.wowzi_airbyte.influencer_smileidentity_data` b
on a.influencer_id = b.influencer_id
and b.id_card_type = 'UG_TRAVEL_DOC'
),

job_details_with_activity_stats as
(
select
    a.influencer_id,
    a.company_id,
    a.company_name,
    a.industry,
    a.campaign_id,
    a.campaign_end_date,
    a.job_id,
    a.job_offer_date,
    a.task_id,
    a.task_creation_date,
    a.tasks_assigned,
    a.job_status,
    a.completed_tasks,
    a.amount_lcy,
    a.campaign_currency,
    a.amount_usd,
    a.payment_date,
    a.periphery_payment_status,
    a.periphery_job_value_usd,
    a.platfrom_job_value_lcy,
    a.inf_first_name,
    a.inf_last_name,
    a.created,
    a.first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    a.gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.invitation_status,
    a.Instagram_influencer_level, 
    a.Facebook_influencer_level, 
    a.X_influencer_level, 
    a.Linkedin_influencer_level, 
    a.Tiktok_influencer_level,
    a.channel,
    a.influencer_level,
    a.datasource, 
    a.first_job_date, 
    a.id_card_type,
    a.tasks_assigned no_of_tasks,
    case 
        when (id_card_type is not null) then 'Refugee' 
        else 'Non-refugee' 
    end refugee_flag,
    concat(FORMAT_DATETIME("%b", DATETIME(date(job_offer_date))),"-", extract(year from date(job_offer_date))) mon_yr,
    case 
        when days_since_last_campaign <= 90 then influencer_id 
        else null 
    end active_l3m,
    case 
        when (days_since_last_campaign <= 90) and (completed_tasks = 1) then influencer_id 
        else null 
    end active_l3m_completed_tasks,
    case 
        when (days_since_last_campaign <= 90) and (completed_tasks = 1) then amount_usd 
        else null 
    end amount_paid_to_active_l3m,
    date_diff(first_job_date, created, DAY) creation_to_job_days,
    case
        when DATE(DATE_TRUNC(date(created), MONTH)) = DATE(DATE_TRUNC(date(first_job_date), MONTH))
        then true 
        else false
    end job_in_reg_month
from creator_refugee_status a),

final_output as
(select 
    a.influencer_id,
    a.company_id,
    a.industry,
    a.campaign_id,
    a.campaign_end_date,
    a.job_id,
    a.job_offer_date,
    a.task_id,
    a.task_creation_date,
    a.tasks_assigned,
    a.job_status,
    a.completed_tasks,
    a.amount_lcy,
    a.campaign_currency,
    a.amount_usd,
    a.payment_date,
    a.periphery_payment_status,
    a.periphery_job_value_usd,
    a.platfrom_job_value_lcy,
    i.currency_rate,
    COALESCE(
        (case 
        when datasource = 'Periphery Sheet' then periphery_job_value_usd
        when datasource = 'Platform' then platfrom_job_value_lcy/i.currency_rate
        else 0 
        end), 0.0) payment_amount_list_usd,
    a.inf_first_name,
    a.inf_last_name,
    a.created,
    a.first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    a.gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.invitation_status,
    a.Instagram_influencer_level, 
    a.Facebook_influencer_level, 
    a.X_influencer_level, 
    a.Linkedin_influencer_level, 
    a.Tiktok_influencer_level,
    case
        when a.channel is null then 'Instagram'
        else a.channel
    end channel,
    case
        when a.influencer_level is null and a.datasource = 'Periphery Sheet'
        then 'Macro'
        when a.influencer_level is null and a.datasource = 'Platform'
        then 'Nano'
        when a.influencer_level = 'None' and a.datasource = 'Platform'
        then 'Nano'
        else a.influencer_level
    end influencer_level,
    a.datasource, 
    a.first_job_date, 
    a.id_card_type,
    a.no_of_tasks,
    a.refugee_flag,
    a.mon_yr,
    a.active_l3m,
    a.active_l3m_completed_tasks,
    a.amount_paid_to_active_l3m,
    a.creation_to_job_days,
    a.job_in_reg_month,
    a.company_name,
    null role, 
    null company_role,
    dense_rank () over (order by extract(year from a.job_offer_date) asc, extract(month from a.job_offer_date)asc) mon_yr_rnk,
    dense_rank () over (order by extract(year from a.payment_date) asc, extract(month from a.payment_date)asc) payment_monyr_rnk,
    row_number() over(partition by a.influencer_id, a.mon_yr order by a.task_id desc) distinct_inf
from job_details_with_activity_stats a 
left join `bi-staging-1-309112.wowzi_dbt_prod.int_currency_rates` i 
on (date(a.task_creation_date) = date(i.date))
and (lower(a.campaign_currency)=lower(i.currency))),

all_inf_details as 
(select 
  safe_cast(influencer_id as string) influencer_id_a, 
  d.job_eligibility,
  'Platform' as datasource
from bi-staging-1-309112.wowzi_dbt_prod.influencer_facts d
union all 
SELECT 
    safe_cast(p.influencer_id as string) influencer_id_a, 
    true job_eligibility,
    'Periphery Sheet' as datasource
FROM `bi-staging-1-309112.wowzi_dbt_prod.periphery_markets_data_clean` p),

qualified_inf as 
(select
    count(distinct influencer_id_a) as qualified_inf
from all_inf_details
    where job_eligibility is true)

select 
    a.influencer_id,
    a.company_id,
    a.industry,
    a.campaign_id,
    a.campaign_end_date,
    a.job_id,
    a.job_offer_date,
    a.task_id,
    a.task_creation_date,
    a.tasks_assigned,
    a.job_status,
    a.completed_tasks,
    a.amount_lcy,
    a.campaign_currency,
    a.amount_usd,
    a.payment_date,
    a.periphery_payment_status,
    a.periphery_job_value_usd,
    a.platfrom_job_value_lcy,
    a.currency_rate,
    case 
        when a.amount_usd > 0 then a.amount_usd
        WHEN a.payment_amount_list_usd = 0 AND lower(country) = 'angola'
        AND a.datasource = 'Platform' THEN a.platfrom_job_value_lcy*0.0012
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'botswana'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.073
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'ethiopia'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.017
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'kenya'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.0077
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'mauritius'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.022
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'mozambique'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.016
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'namibia'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.054
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'south africa'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.054
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'tanzania'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.00038
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'tanzania'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.00026
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'zambia'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*0.037
        WHEN a.payment_amount_list_usd = 0 and lower(country) = 'zimbabwe'
        AND a.datasource = 'Platform' THEN  a.platfrom_job_value_lcy*1
        else a.payment_amount_list_usd
    end payment_amount_list_usd,
    a.inf_first_name,
    a.inf_last_name,
    a.created,
    a.first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.days_since_last_campaign,
    case 
        when lower(a.gender) <> 'male' and lower(a.gender) <> 'female'
        then 'Male'
        else a.gender
    end gender,
    a.dob,
    a.inf_age,
    a.age_groups,
    a.country,
    a.invitation_status,
    a.Instagram_influencer_level, 
    a.Facebook_influencer_level, 
    a.X_influencer_level, 
    a.Linkedin_influencer_level, 
    a.Tiktok_influencer_level,
    a.channel,
    a.influencer_level,
    a.datasource, 
    a.first_job_date, 
    a.id_card_type,
    a.no_of_tasks,
    a.refugee_flag,
    a.mon_yr,
    a.active_l3m,
    a.active_l3m_completed_tasks,
    a.amount_paid_to_active_l3m,
    a.creation_to_job_days,
    a.job_in_reg_month,
    a.company_name,
    a.role, 
    a.company_role,
    a.mon_yr_rnk,
    a.payment_monyr_rnk,
    a.distinct_inf,
    b.qualified_inf all_time_qualified_inf
from final_output a
left join qualified_inf b on 
b.qualified_inf = b.qualified_inf
    where a.influencer_id is not null
    and a.company_id not in 
    (9,17744,17364,17394,17395,16035,5555,17940,17615,16451,16084,5747,4794,16032)