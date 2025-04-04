git brancWITH inf AS
(
SELECT 
  influencer_id,
  COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS full_name,
  initcap(f.gender) gender,
  email,
  mobile_number,
  initcap(f.location) location,
  f.age,
  f.age_range,
  c.Country country,
  CASE
    WHEN channel_status_INSTAGRAM = 'APPROVED' then username_INSTAGRAM
    ELSE NULL 
  END username_INSTAGRAM,
  CASE
    WHEN channel_status_FACEBOOK = 'APPROVED' then username_FACEBOOK
    ELSE NULL 
  END username_FACEBOOK,
  job_activity,
  last_campaign_date
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.country_key`c ON lower(f.country) = lower(c.Key)
  where job_eligibility is true
  -- and lower(f.country) = 'ke'
),

inf2 AS
(
SELECT 
  influencer_id,
  full_name,
  gender,
  email,
  mobile_number,
  country,
  location,
  age,
  age_range,
  username_INSTAGRAM,
  username_FACEBOOK,
  job_activity,
  last_campaign_date
FROM inf
WHERE NOT (username_INSTAGRAM IS NULL AND username_FACEBOOK IS NULL)
),

ongoing_jobs AS 
(
select 
  influencer_id,
  ongoing_ig_job,
  ongoing_fb_job,
  count(distinct job_id) num_ongoing_jobs
from
(SELECT 
  influencer_id,
  CASE
    WHEN lower(channel) = 'instagram' THEN TRUE 
    ELSE FALSE 
  END ongoing_ig_job,
  CASE
    WHEN lower(channel) = 'facebook' THEN TRUE 
    ELSE FALSE 
  END ongoing_fb_job,
  job_id
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
where lower(job_status) = 'ongoing'
and lower(channel) = 'instagram'
-- and lower(channel) in ('instagram', 'facebook')
)
group by influencer_id,ongoing_ig_job,ongoing_fb_job
),

inf3 AS
(
SELECT 
  a.influencer_id,
  a.full_name,
  a.gender,
  a.email,
  a.mobile_number,
  a.country,
  a.location,
  a.age,
  a.age_range,
  a.username_INSTAGRAM,
  b.ongoing_ig_job,
  a.username_FACEBOOK,
  b.ongoing_fb_job,
  a.job_activity,
  a.last_campaign_date,
  b.num_ongoing_jobs num_ongoing_jobs
FROM inf2 a 
LEFT JOIN ongoing_jobs b ON a.influencer_id = b.influencer_id
),

all_meta_jobs AS 
(
select
  influencer_id,
  fb_jobs,
  ig_jobs,
  fb_jobs+ig_jobs meta_jobs
from
(
SELECT 
  distinct
  influencer_id,
  (select count(distinct job_id)
  from `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
  where lower(job_status) in ('ongoing', 'complete')
  and lower(channel) in ('instagram') ) ig_jobs,
  (select count(distinct job_id)
  from `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
  where lower(job_status) in ('ongoing', 'complete')
  and lower(channel) in ('facebook') ) fb_jobs
FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
where lower(job_status) in ('ongoing', 'complete')
and lower(channel) in ('instagram', 'facebook')
)
),
link_sts as 
(
select CAST(influencer_id AS INT64) inf_id, ig_id, date(processed_at) as linked_date 
from bi-staging-1-309112.wowzi_dynamo_exports.instagram_social_media_linking
),
final_table as 
(
SELECT 
  DISTINCT
  i.influencer_id,
  initcap(i.full_name) full_name,
  i.gender,
  i.email,
  i.mobile_number,
  i.country,
  i.location,
  i.age,
  i.age_range,
  i.username_INSTAGRAM,
  i.ongoing_ig_job,
  am.ig_jobs all_ig_jobs,
  i.username_FACEBOOK,
  am.fb_jobs all_fb_jobs,
  i.ongoing_fb_job,
  i.job_activity,
  am.meta_jobs all_meta_jobs,
  i.last_campaign_date,
  i.num_ongoing_jobs,
  CASE 
    WHEN num_ongoing_jobs > 0 then TRUE
    ELSE FALSE
  END has_ongoing_meta_job,
  CASE 
    WHEN meta_jobs > 0 then TRUE
    ELSE FALSE
  END has_done_a_meta_job,
  l.inf_id linked
FROM inf3 i 
LEFT JOIN all_meta_jobs am ON i.influencer_id = am.influencer_id
LEFT JOIN link_sts l on i.influencer_id = l.inf_id
)

SELECT 
  ROW_NUMBER() OVER (
    ORDER BY 
      CASE 
        -- Highest Priority: Female influencers outside Nairobi, under 35 with ongoing jobs
        WHEN gender = 'Female' AND location <> 'Nairobi' AND age < 35 AND num_ongoing_jobs > 0 THEN 1 

        -- Second Priority: Female influencers outside Nairobi, under 35 with past meta jobs
        WHEN gender = 'Female' AND location <> 'Nairobi' AND age < 35 AND all_meta_jobs > 0 THEN 2 
        
        -- Third Priority: All other conditions
        WHEN num_ongoing_jobs > 0 THEN 3  
        WHEN all_meta_jobs > 0 THEN 4     
        ELSE 5  
      END,
      num_ongoing_jobs DESC,      -- Sort ongoing jobs in descending order
      last_campaign_date DESC     -- Sort remaining rows by last_campaign_date in descending order
  ) AS new_rnk,

  ROW_NUMBER() OVER (
    ORDER BY 
      CASE 
        WHEN num_ongoing_jobs > 0 THEN 1  -- Highest priority: num_ongoing_jobs > 0
        WHEN all_meta_jobs > 0 THEN 2     -- Second priority: last_campaign_date for all_meta_jobs > 0
        ELSE 3                            -- Lowest priority: last_campaign_date for all remaining rows
      END,
      num_ongoing_jobs DESC,      -- Sort ongoing jobs in descending order
      last_campaign_date DESC     -- Sort remaining rows by last_campaign_date in descending order
  ) AS row_num,

  influencer_id,
  full_name,
  email,
  mobile_number,
  s.outreach_status,
  country,
  location,
  username_INSTAGRAM,
  ongoing_ig_job,
  all_ig_jobs,
  username_FACEBOOK,
  all_fb_jobs,
  ongoing_fb_job,
  job_activity,
  all_meta_jobs,
  last_campaign_date,
  num_ongoing_jobs,
  has_ongoing_meta_job,
  has_done_a_meta_job,
  case 
    when linked is not null then 'Linked'
    else 'Not Linked'
  end as linked_sts
FROM final_table f
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.gsheets_stg__social_media_linking_outreach` s 
ON CAST(f.influencer_id AS STRING) = CAST(s.creator_id AS STRING)

-- where influencer_id = 140709

ORDER BY 
new_rnk