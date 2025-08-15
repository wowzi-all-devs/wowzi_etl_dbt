/*This query on the other hand leaves a static group of 500 creators who have not 
linked their social media accounts. I did this to allow for easy measurement of creator linking 
within each group.
*/


WITH base AS (
  SELECT
  influencer_id,
  first_name,
  gender,
  email,
  mobile_number,
  country,
  location,
  age,
  age_range,
  job_activity,
  job_eligibility,
  smileidentity_status,
  completed_job_l1y,
  case when instagram_linked is null then 'not-linked' else instagram_linked end as instagram_linked,
  case when facebook_linked is null then 'not-linked' else facebook_linked end as facebook_linked,
  case when tiktok_linked is null then 'not-linked' else tiktok_linked end as tiktok_linked,
  case when twitter_linked is null then 'not-linked' else twitter_linked end as twitter_linked,
  case when linkedin_linked is null then 'not-linked' else linkedin_linked end as linkedin_linked,
  is_system_linked,
  linked_date,
  ROW_NUMBER() OVER (PARTITION BY influencer_id) AS rnk
  FROM (
    WITH
      inf AS (
        SELECT
          influencer_id,
          -- COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS full_name,
          initcap(first_name) first_name,
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
          INITCAP(smileidentity_status) smileidentity_status,
          job_eligibility
        FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
        LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.country_key` c
          ON LOWER(f.country) = LOWER(c.Key)
        WHERE job_eligibility IS TRUE
      ),
      inf2 AS (
        SELECT
          SAFE_CAST(influencer_id AS INT64) influencer_id,
          INITCAP(first_name) first_name,
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
        FROM inf
      ),
      jobs_l1y AS (
        SELECT influencer_id
        FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
        WHERE LOWER(job_status) = 'complete'
          AND DATE(task_creation_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) 
          AND CURRENT_DATE()
      ),
      inf3 AS (
        SELECT 
          inf2.*,
          CASE 
            WHEN jobs_l1y.influencer_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
          END AS completed_job_l1y
        FROM inf2
        LEFT JOIN jobs_l1y
          ON inf2.influencer_id = jobs_l1y.influencer_id
      ),
      linked_acc AS (
                  SELECT
                    SAFE_CAST(influencer_id AS INT64) influencer_id,
                    is_system_linked,
                    MAX(DATE(status_update_time)) AS linked_date,
                    COALESCE(MAX(CASE WHEN LOWER(channel) = 'instagram' THEN 'linked' END), 'not-linked') AS instagram_linked,
                    coalesce(MAX(CASE WHEN LOWER(channel) = 'facebook' THEN 'linked' END), 'not-linked') AS facebook_linked,
                    COALESCE(MAX(CASE WHEN LOWER(channel) = 'tiktok' THEN 'linked' END), 'not-linked') AS tiktok_linked,
                    coalesce(MAX(CASE WHEN LOWER(channel) = 'twitter' THEN 'linked' END), 'not-linked') AS twitter_linked,
                    coalesce(MAX(CASE WHEN LOWER(channel) = 'linkedin' THEN 'linked' END), 'not-linked') AS linkedin_linked 
                  from bi-staging-1-309112.wowzi_dbt_prod.postgres_stg__influencer_channel_data 
                  WHERE LOWER(status) = 'approved'
                  -- and influencer_id = 113483
                  GROUP BY 1, 2
        --   SELECT
        --   SAFE_CAST(influencer_id AS INT64) influencer_id,
        --   MAX(DATE(updated_at)) AS linked_date,
        --   COALESCE(MAX(CASE WHEN LOWER(GSI1SK) = 'instagram' THEN 'linked' END), 'not-linked') AS instagram_linked,
        --  coalesce(MAX(CASE WHEN LOWER(GSI1SK) = 'facebook' THEN 'linked' END), 'not-linked') AS facebook_linked
        -- FROM bi-staging-1-309112.custom_pipe_eu.socials
        -- WHERE LOWER(GSI1SK) IN ('facebook', 'instagram')
        -- GROUP BY 1
      )
      -- username AS (
      --   SELECT 
      --     influencer_id,
      --     MAX(CASE WHEN LOWER(channel) = 'tiktok' THEN username ELSE NULL END) AS tiktok_username,
      --     MAX(CASE WHEN LOWER(channel) = 'facebook' THEN username ELSE NULL END) AS facebook_username,
      --     MAX(CASE WHEN LOWER(channel) = 'twitter' THEN username ELSE NULL END) AS twitter_username,
      --     MAX(CASE WHEN LOWER(channel) = 'linkedin' THEN username ELSE NULL END) AS linkedin_username
      --   FROM bi-staging-1-309112.wowzi_airbyte.influencer_channel_data
      --   WHERE LOWER(status) = 'approved'
      --   GROUP BY 1
      -- )
    SELECT inf3.*,
    linked_acc.linked_date,
    linked_acc.instagram_linked,
    linked_acc.facebook_linked,
    linked_acc.tiktok_linked,
    linked_acc.twitter_linked,
    linked_acc.linkedin_linked,
    linked_acc.is_system_linked
    FROM inf3
    LEFT JOIN linked_acc 
      ON inf3.influencer_id = linked_acc.influencer_id
    -- LEFT JOIN username
    --   ON inf3.influencer_id = username.influencer_id
  ) final
),
base2 as
(
select
base.*
from base
where rnk =1
),
unlinked_snapshot AS (
  SELECT 
    influencer_id,
    CEIL(ROW_NUMBER() OVER (ORDER BY influencer_id DESC) / 500.0) AS group_id
  FROM base2
  WHERE mobile_number IS NOT NULL
)

SELECT 
  b.influencer_id,
  b.first_name,
  b.gender,
  b.email,
  CASE 
    WHEN s.group_id IS NOT NULL 
         AND b.instagram_linked = 'not-linked' 
         AND b.facebook_linked = 'not-linked' 
         AND b.tiktok_linked = 'not-linked'
         AND b.twitter_linked = 'not-linked'
    THEN b.mobile_number
    ELSE NULL
  END AS mobile_number,
  b.country,
  b.location,
  b.age,
  b.age_range,
  b.job_activity,
  b.job_eligibility,
  b.completed_job_l1y,
  -- b.facebook_username,
  -- b.twitter_username,
  -- b.tiktok_username,
  -- b.linkedin_username,
  b.instagram_linked,
  b.facebook_linked,
  b.tiktok_linked,
  b.twitter_linked,
  b.linkedin_linked,
  b.is_system_linked,
  b.linked_date,
  s.group_id
FROM base2 b
LEFT JOIN unlinked_snapshot s
  ON b.influencer_id = s.influencer_id





---This dynamically ranks creators who have not linked their social media accounts and returns their mobile numbers in groups of 500.
-- The query also takes out creators who have linked and assigns a new creator into that rank.

-- WITH base AS (
--   SELECT
--   influencer_id,
--   full_name,
--   gender,
--   email,
--   mobile_number,
--   country,
--   location,
--   age,
--   age_range,
--   job_activity,
--   job_eligibility,
--   smileidentity_status,
--   completed_job_l1y,
--   case when instagram_linked is null then 'not-linked' else instagram_linked end as instagram_linked,
--   case when facebook_linked is null then 'not-linked' else facebook_linked end as facebook_linked,
--   linked_date,
--   ROW_NUMBER() OVER (PARTITION BY influencer_id) AS rnk
--   FROM (
--     WITH
--       inf AS (
--         SELECT
--           influencer_id,
--           COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS full_name,
--           INITCAP(f.gender) gender,
--           email,
--           mobile_number,
--           INITCAP(f.location) location,
--           f.age,
--           f.age_range,
--           c.Country country,
--           job_activity,
--           last_campaign_date,
--           date_account_created,
--           INITCAP(smileidentity_status) smileidentity_status,
--           job_eligibility
--         FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
--         LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.country_key` c
--           ON LOWER(f.country) = LOWER(c.Key)
--         WHERE job_eligibility IS TRUE
--       ),
--       inf2 AS (
--         SELECT
--           SAFE_CAST(influencer_id AS INT64) influencer_id,
--           INITCAP(full_name) full_name,
--           gender,
--           email,
--           mobile_number,
--           country,
--           location,
--           age,
--           age_range,
--           job_activity,
--           job_eligibility,
--           date_account_created,
--           smileidentity_status,
--           last_campaign_date
--         FROM inf
--       ),
--       jobs_l1y AS (
--         SELECT influencer_id
--         FROM `bi-staging-1-309112.wowzi_dbt_prod.influencer_job_breakdown` 
--         WHERE LOWER(job_status) = 'complete'
--           AND DATE(task_creation_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) 
--           AND CURRENT_DATE()
--       ),
--       inf3 AS (
--         SELECT 
--           inf2.*,
--           CASE 
--             WHEN jobs_l1y.influencer_id IS NOT NULL THEN TRUE 
--             ELSE FALSE 
--           END AS completed_job_l1y
--         FROM inf2
--         LEFT JOIN jobs_l1y
--           ON inf2.influencer_id = jobs_l1y.influencer_id
--       ),
--       linked_acc AS (
--         SELECT
--           SAFE_CAST(influencer_id AS INT64) influencer_id,
--           max(date(updated_at)) linked_date,
--           MAX(CASE WHEN LOWER(GSI1SK) = 'instagram' THEN 'linked' ELSE 'not-linked' END) AS instagram_linked,
--           MAX(CASE WHEN LOWER(GSI1SK) = 'facebook' THEN 'linked' ELSE 'not-linked' END) AS facebook_linked
--         FROM bi-staging-1-309112.custom_pipe_eu.socials
--         WHERE GSI1SK NOT LIKE 'INFLUENCER%'
--         GROUP BY 1
--       )
--       -- username AS (
--       --   SELECT 
--       --     influencer_id,
--       --     MAX(CASE WHEN LOWER(channel) = 'tiktok' THEN username ELSE NULL END) AS tiktok_username,
--       --     MAX(CASE WHEN LOWER(channel) = 'facebook' THEN username ELSE NULL END) AS facebook_username,
--       --     MAX(CASE WHEN LOWER(channel) = 'twitter' THEN username ELSE NULL END) AS twitter_username,
--       --     MAX(CASE WHEN LOWER(channel) = 'linkedin' THEN username ELSE NULL END) AS linkedin_username
--       --   FROM bi-staging-1-309112.wowzi_airbyte.influencer_channel_data
--       --   WHERE LOWER(status) = 'approved'
--       --   GROUP BY 1
--       -- )
--     SELECT inf3.*,
--     linked_acc.linked_date,
--     linked_acc.instagram_linked,
--     linked_acc.facebook_linked
--     FROM inf3
--     LEFT JOIN linked_acc 
--       ON inf3.influencer_id = linked_acc.influencer_id
--     -- LEFT JOIN username
--     --   ON inf3.influencer_id = username.influencer_id
--   ) final
-- ),
-- base2 as
-- (
-- select
-- base.*
-- from base
-- where rnk =1
-- ),
-- unlinked_ranked AS (
--   SELECT 
--     influencer_id,
--     DENSE_RANK() OVER (ORDER BY influencer_id DESC) AS unlinked_rnk
--   FROM base2
--   WHERE instagram_linked = 'not-linked'
--     AND facebook_linked = 'not-linked'
--     AND mobile_number IS NOT NULL
-- )
-- SELECT 
--   b.influencer_id,
--   b.full_name,
--   b.gender,
--   b.email,
--   CASE 
--     WHEN u.unlinked_rnk <= 500 THEN b.mobile_number
--     ELSE NULL
--   END AS mobile_number,
--   b.country,
--   b.location,
--   b.age,
--   b.age_range,
--   b.job_activity,
--   b.job_eligibility,
--   b.completed_job_l1y,
--   b.instagram_linked,
--   b.facebook_linked,
--   b.linked_date,
--   u.unlinked_rnk
-- FROM base2 b
-- LEFT JOIN unlinked_ranked u
--   ON b.influencer_id = u.influencer_id