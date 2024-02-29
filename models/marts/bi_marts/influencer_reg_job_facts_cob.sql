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
  d.age, 
  d.job_eligibility,
  d.date_account_created,
  d.influencer_type_FACEBOOK facebook_inf_level,
  d.influencer_type_INSTAGRAM instagram_inf_level,
  d.influencer_type_TIKTOK tiktok_inf_level,
  d.influencer_type_TWITTER twitter_inf_level,
  d.influencer_type_LINKEDIN linkedin_inf_level,
  d.channel_status_FACEBOOK facebook_status,
  d.channel_status_INSTAGRAM instagram_status,
  d.channel_status_TIKTOK tiktok_status,
  d.channel_status_TWITTER twitter_status,
  d.channel_status_LINKEDIN linkedin_status,
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
    case when p.social_media_platform = 'FACEBOOK' then p.influencer_level
    else null end facebook_inf_level,
    case when p.social_media_platform = 'INSTAGRAM' then p.influencer_level
    else null end instagram_inf_level,
    case when p.social_media_platform = 'TIKTOK' then p.influencer_level
    else null end tiktok_inf_level,
    case when p.social_media_platform = 'TWITTER' then p.influencer_level
    else null end  twitter_inf_level,
    case when p.social_media_platform = 'LINKEDIN' then p.influencer_level
    else null end  linkedin_inf_level,
    case when p.social_media_platform = 'FACEBOOK' then 'APPROVED'
    else null end facebook_status,
    case when p.social_media_platform = 'INSTAGRAM' then 'APPROVED'
    else null end instagram_status,
    case when p.social_media_platform = 'TIKTOK' then 'APPROVED'
    else null end tiktok_status,
    case when p.social_media_platform = 'TWITTER' then 'APPROVED'
    else null end twitter_status,
    case when p.social_media_platform = 'LINKEDIN' then 'APPROVED'
    else null end linkedin_status,
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
  email, 
  gender,
  smileidentity_status, 
  age, 
  job_eligibility,
  date_account_created,
  facebook_inf_level,
  instagram_inf_level,
  tiktok_inf_level,
  twitter_inf_level,
  linkedin_inf_level,
  facebook_status,
  instagram_status,
  tiktok_status,
  twitter_status,
  linkedin_status,
  inf_first_campaign_date,
  inf_last_campaign_date,
  case 
    when i.country is null then c.Country
    else i.country
  end country,
  c.Country clean_country,
  acc_cre_mon_yr,
  acc_cre_mon,
  acc_cre_yr,
  dense_rank () over (order by extract(year from date_account_created) asc, extract(month from date_account_created)asc ) acc_cre_rnk,
  datasource
from inf_details i
left join bi-staging-1-309112.wowzi_dbt_prod.country_key c 
on cast(left(i.mobile_number, 4) as string) = cast(c.Dial_code as string)
),

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
    left(i.mobile_number, 4) as dial_code,
    i.email, 
    i.gender,
    i.smileidentity_status, 
    i.age, 
    i.job_eligibility,
    i.date_account_created,
    i.facebook_inf_level,
    i.instagram_inf_level,
    i.tiktok_inf_level,
    i.twitter_inf_level,
    i.linkedin_inf_level,
    i.facebook_status,
    i.instagram_status,
    i.tiktok_status,
    i.twitter_status,
    i.linkedin_status,
    i.inf_first_campaign_date,
    i.inf_last_campaign_date,
    DATE_DIFF(date(i.inf_first_campaign_date), date(i.date_account_created), day) as days_to_job,
    i.country,
    i.clean_country,
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

all_influencer_details as
(select 
    a.influencer_id_a, 
    a.inf_age_range,
    a.first_name, 
    a.last_name,
    a.mobile_number,
    a.dial_code,
    a.email, 
    a.gender,
    a.smileidentity_status, 
    a.age, 
    a.job_eligibility,
    a.date_account_created,
    a.facebook_inf_level,
    a.instagram_inf_level,
    a.tiktok_inf_level,
    a.twitter_inf_level,
    a.linkedin_inf_level,
    a.facebook_status,
    a.instagram_status,
    a.tiktok_status,
    a.twitter_status,
    a.linkedin_status,
    a.inf_first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    a.country,
    a.clean_country,
    a.acc_cre_mon_yr,
    a.acc_cre_mon,
    a.acc_cre_yr,
    a.acc_cre_rnk,
    a.datasource,
    a.parent_category_id,
    a.parent_category,
    a.subcategory_id,
    a.enum_value
from inf_details_ranked2 a),

influencers_with_complete_jobs as
(SELECT  
  distinct 
  influencer_id 
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
  where influencer_id is not null
  and job_status in ('Complete', 'Ongoing')),

gender_fill as
(select 
  b.influencer_id_a,
  b.rownum,
  case when b.rownum < c.num then 'Male'
  else 'Female'
  end as gender_fill
from
(select
  influencer_id_a,
  row_number() over(order by influencer_id_a) as rownum
from
(SELECT 
  distinct
  influencer_id_a
FROM all_influencer_details
  where influencer_id_a is not null
  and gender is null) a 
  order by influencer_id_a) b
  left join 
  (SELECT 
  cast(count(distinct influencer_id_a)*0.7 as int) as num
FROM all_influencer_details
  where influencer_id_a is not null
  and gender is null) c on c.num = c.num)

select 
    a.influencer_id_a, 
    a.inf_age_range,
    a.first_name, 
    a.last_name,
    a.mobile_number,
    a.dial_code,
    a.email, 
    case 
      when a.gender is null then g.gender_fill
      when lower(a.gender) <> 'male' and lower(a.gender) <> 'female'
      then 'Male'
      else a.gender 
    end gender,
    a.smileidentity_status, 
    a.age, 
    a.job_eligibility,
    a.date_account_created,
    case when a.date_account_created >= date_trunc(date_add(a.date_account_created, interval -12 month), month)
    then 1
    else 0 end date_account_created_filter,
    case when a.facebook_inf_level is null then 'NANO'
    when a.facebook_inf_level = 'NONE' then 'NANO'
    else a.facebook_inf_level end facebook_inf_level,
    case when a.instagram_inf_level is null then 'NANO'
    when a.instagram_inf_level = 'NONE' then 'NANO'
    else a.instagram_inf_level end instagram_inf_level,
    case when a.tiktok_inf_level is null then 'NANO'
    when a.tiktok_inf_level = 'NONE' then 'NANO'
    else a.tiktok_inf_level end tiktok_inf_level,
    case when a.twitter_inf_level is null then 'NANO'
    when a.twitter_inf_level = 'NONE' then 'NANO'
    else a.twitter_inf_level end twitter_inf_level,
    case when a.linkedin_inf_level is null then 'NANO'
    when a.linkedin_inf_level = 'NONE' then 'NANO'
    else a.linkedin_inf_level end linkedin_inf_level,
    a.facebook_status,
    a.instagram_status,
    a.tiktok_status,
    a.twitter_status,
    a.linkedin_status,
    a.inf_first_campaign_date,
    a.inf_last_campaign_date,
    a.days_to_job,
    case 
      when a.country is null then 'Kenya'
      else a.country 
    end country,
    a.clean_country,
    a.acc_cre_mon_yr,
    a.acc_cre_mon,
    a.acc_cre_yr,
    a.acc_cre_rnk,
    a.datasource,
    a.parent_category_id,
    a.parent_category,
    a.subcategory_id,
    a.enum_value inf_profession,
    case 
      when c.influencer_id is not null then true 
      else false
    end completed_one_job
from all_influencer_details a
left join influencers_with_complete_jobs c on cast(a.influencer_id_a as string) = cast(c.influencer_id as string)
left join gender_fill g on cast(a.influencer_id_a as string) = cast(g.influencer_id_a as string)
