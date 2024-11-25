with cohort_items as 
(
  SELECT
    min(cohort_quarter) cohort_quarter,
    min(yr) cohort_year,
    min(qtr_yr_rnk) qtr_yr_rnk,
    influencer_id
FROM
(
  SELECT  
    influencer_id,
    extract(year from job_offer_date) yr,
    dense_rank () over (order by extract(year from job_offer_date) asc, extract(quarter from job_offer_date)asc ) qtr_yr_rnk,
    CONCAT(
      CAST(EXTRACT(YEAR FROM date(job_offer_date)) AS STRING),
      ' Q', EXTRACT(QUARTER FROM date(job_offer_date))) AS cohort_quarter,
    extract(quarter from job_offer_date) quarter_no
  FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
    where influencer_id is not null
    and job_status in ('Complete', 'Ongoing')
    and lower(influencer_level) = 'micro'
) 
  group by influencer_id
  order by cohort_year, qtr_yr_rnk, cohort_quarter, influencer_id
),

influencer_activities as 
(select
  distinct 
  a.influencer_id,
  a.yr job_offer_year,
  a.qtr_yr_rnk job_offer_qtr_yr_rnk,
  a.cohort_quarter job_offer_qtr,
  a.qtr_yr_rnk - b.qtr_yr_rnk qtr_no
  --date_diff(a.cohort_quarter, b.cohort_quarter, month) qtr_no
from
(
  select 
      a.influencer_id,
      extract(year from job_offer_date) yr,
      dense_rank () over (order by extract(year from job_offer_date) asc, extract(quarter from job_offer_date)asc ) qtr_yr_rnk,
      CONCAT(
        CAST(EXTRACT(YEAR FROM date(job_offer_date)) AS STRING),
        ' Q', EXTRACT(QUARTER FROM date(job_offer_date))) AS cohort_quarter,
      extract(quarter from job_offer_date) quarter_no
  from bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown a
    where influencer_id is not null
    and job_status in ('Complete', 'Ongoing')
    and lower(influencer_level) = 'micro'
) a
left join cohort_items b on a.influencer_id = b.influencer_id
),


cohort_size as 
(
  select 
      a.cohort_year,
      a.cohort_quarter,
      a.qtr_yr_rnk,
      count(a.influencer_id) num_influencers
  from cohort_items a
      group by cohort_year, qtr_yr_rnk, cohort_quarter
      order by cohort_year, qtr_yr_rnk, cohort_quarter
),

retention_table as
(
  select
      b.cohort_year,
      b.qtr_yr_rnk,
      b.cohort_quarter,
      a.qtr_no,
      count(a.influencer_id) num_influenccers
  from influencer_activities a 
  left join cohort_items b on a.influencer_id =b.influencer_id
      group by b.cohort_year, b.qtr_yr_rnk, b.cohort_quarter, a.qtr_no
)

select 
    a.cohort_year, 
    a.qtr_yr_rnk, 
    a.cohort_quarter,
    b.num_influencers total_inf,
    a.qtr_no,
    a.num_influenccers active_inf,
    cast(a.num_influenccers as numeric)/b.num_influencers retention_rate
from retention_table a
left join cohort_size b on a.cohort_quarter = b.cohort_quarter
    where a.cohort_quarter is not null
    order by a.qtr_yr_rnk,a.qtr_no, a.cohort_quarter

