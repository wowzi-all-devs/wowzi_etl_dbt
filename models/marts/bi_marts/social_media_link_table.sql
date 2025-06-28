WITH
  inf AS (
  SELECT
    influencer_id,
    COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS full_name,
    INITCAP(f.gender) gender,
    email,
    mobile_number,
    INITCAP(f.location) location,
    f.age,
    f.age_range,
    c.Country country,
    job_activity,
    last_campaign_date,
    date_account_created,
    initcap(smileidentity_status) smileidentity_status,
    job_eligibility
  FROM
    `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
  LEFT JOIN
    `bi-staging-1-309112.wowzi_dbt_prod.country_key`c
  ON
    LOWER(f.country) = LOWER(c.Key)
  WHERE
    job_eligibility IS TRUE
    -- and lower(f.country) = 'ke'
    ),
  inf2 AS (
  SELECT
    safe_cast(influencer_id as INT64) influencer_id,
    full_name,
    gender,
    email,
    mobile_number,
    country,
    location,
    age,
    age_range,
    job_activity,
    job_eligibility,
    date_account_created,
    smileidentity_status,
    last_campaign_date
  FROM
    inf
    -- WHERE NOT (username_INSTAGRAM IS NULL AND username_FACEBOOK IS NULL)
  ),
linked_acc as
(
  SELECT
  safe_cast(influencer_id as INT64) influencer_id,
  MAX(CASE  WHEN LOWER(GSI1SK) = 'instagram' THEN "linked"  ELSE NULL END ) instagram_linked,
  MAX(CASE WHEN LOWER(GSI1SK) = 'facebook' THEN "linked" ELSE NULL END ) facebook_linked
FROM
  bi-staging-1-309112.custom_pipe_eu.socials
WHERE
  GSI1SK NOT LIKE 'INFLUENCER%'
  -- AND influencer_id = '33254'
GROUP BY
  1
),
username as
(
select 
influencer_id,
max(case when lower(channel) = 'tiktok' then username else null end) tiktok_username,
max(case when lower(channel) = 'facebook' then username else null end) facebook_username,
max(case when lower(channel) = 'twitter' then username else null end) twitter_username,
max(case when lower(channel) = 'linkedin' then username else null end) linkedin_username
FROM
  bi-staging-1-309112.wowzi_airbyte.influencer_channel_data
WHERE
-- influencer_id = 162713
  lower(status) = 'approved'
GROUP BY 1
)
select 
inf2.*,
username.facebook_username,
username.twitter_username,
username.tiktok_username,
username.linkedin_username,
instagram_linked,
facebook_linked 
from inf2 
left join 
  linked_acc 
on 
  inf2.influencer_id = linked_acc.influencer_id
-- WHERE 
--   instagram_linked is not null or facebook_linked is not null
left join 
  username
  on 
  inf2.influencer_id = username.influencer_id
--   WHERE 
--   inf2.influencer_id = 162713
  -- instagram_linked is not null or facebook_linked is not null