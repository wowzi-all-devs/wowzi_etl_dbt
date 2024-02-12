with cohort_items as 
(select
    min(cohort_month) cohort_month,
  influencer_id
from
(SELECT  
  influencer_id,
  cast(date_trunc(date(job_offer_date), month) as date) cohort_month
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
  where influencer_id is not null
  and job_status in ('Complete', 'Ongoing')) 
  group by influencer_id
  order by cohort_month, influencer_id),

influencer_activities as 
(select
  distinct 
  a.influencer_id,
  a.job_offer_month,
  date_diff(a.job_offer_month, b.cohort_month, month) month_number
from
(select 
    a.influencer_id,
    date_trunc(date(job_offer_date), month) job_offer_month
from bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown a
  where influencer_id is not null
  and job_status in ('Complete', 'Ongoing')) a
left join cohort_items b on a.influencer_id = b.influencer_id),

cohort_size as 
(select 
    a.cohort_month,
    count(a.influencer_id) num_influencers
from cohort_items a
    group by cohort_month
    order by cohort_month),

retention_table as
(select
    b.cohort_month,
    a.month_number,
    count(a.influencer_id) num_influenccers
from influencer_activities a 
left join cohort_items b on a.influencer_id =b.influencer_id
    group by b.cohort_month, a.month_number)

select 
    a.cohort_month,
    concat( FORMAT_DATETIME("%b", a.cohort_month ),"-", extract(year from a.cohort_month) ) cohort_mon_yr,
    FORMAT_DATETIME("%b", DATETIME(date(a.cohort_month))) cohort_mon,
    extract(year from a.cohort_month) cohort_yr,
    dense_rank () over (order by extract(year from a.cohort_month) asc, extract(month from a.cohort_month)asc ) cohort_yr_rnk,
    b.num_influencers total_inf,
    a.month_number,
    cast(a.num_influenccers as numeric)/b.num_influencers retention_rate
from retention_table a
left join cohort_size b on a.cohort_month = b.cohort_month
    where a.cohort_month is not null
    order by a.cohort_month, a.month_number
