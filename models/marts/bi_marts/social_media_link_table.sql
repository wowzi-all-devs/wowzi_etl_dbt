  SELECT /*This section desplays the phone numbers of 500 influencers who have not linked their social media accounts*/
  influencer_id,
  full_name,
  gender,
  email,
  CASE 
    WHEN (instagram_linked = 'not-linked' 
         and facebook_linked = 'not-linked') 
         AND unlinked_rnk <= 500
      THEN mobile_number
    ELSE NULL
  END AS mobile_number,
  country,
  location,
  age,
  age_range,
  job_activity,
  job_eligibility,
  completed_job_l1y,
  facebook_username,
  twitter_username,
  tiktok_username,
  linkedin_username,
  instagram_linked,
  facebook_linked
FROM (
  SELECT /*This section adds a rank to influencers who have not linked their social media accounts*/
    *,
    CASE 
      WHEN instagram_linked = 'not-linked' AND facebook_linked = 'not-linked' 
      THEN ROW_NUMBER() OVER (
            ORDER BY influencer_id desc
          )
      ELSE NULL
    END AS unlinked_rnk
  FROM (
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
          initcap(full_name) full_name,
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
          MAX(CASE WHEN LOWER(GSI1SK) = 'instagram' THEN 'linked' ELSE 'not-linked' END) AS instagram_linked,
          MAX(CASE WHEN LOWER(GSI1SK) = 'facebook' THEN 'linked' ELSE 'not-linked' END) AS facebook_linked
        FROM  bi-staging-1-309112.custom_pipe_eu.socials
        WHERE GSI1SK NOT LIKE 'INFLUENCER%'
        GROUP BY 1
      ),
      username AS (
        SELECT 
          influencer_id,
          MAX(CASE WHEN LOWER(channel) = 'tiktok' THEN username ELSE NULL END) AS tiktok_username,
          MAX(CASE WHEN LOWER(channel) = 'facebook' THEN username ELSE NULL END) AS facebook_username,
          MAX(CASE WHEN LOWER(channel) = 'twitter' THEN username ELSE NULL END) AS twitter_username,
          MAX(CASE WHEN LOWER(channel) = 'linkedin' THEN username ELSE NULL END) AS linkedin_username
        FROM bi-staging-1-309112.wowzi_airbyte.influencer_channel_data
        WHERE LOWER(status) = 'approved'
        GROUP BY 1
      )
    SELECT 
      inf3.*,
      username.facebook_username,
      username.twitter_username,
      username.tiktok_username,
      username.linkedin_username,
      case when instagram_linked is null then 'not-linked' else instagram_linked end as instagram_linked,
      case when facebook_linked is null then 'not-linked' else facebook_linked end as facebook_linked,
      ROW_NUMBER() OVER (PARTITION BY inf3.influencer_id) AS rnk
    FROM inf3
    LEFT JOIN linked_acc 
      ON inf3.influencer_id = linked_acc.influencer_id
    LEFT JOIN username
      ON inf3.influencer_id = username.influencer_id
  ) full_data 
where rnk = 1
) final
WHERE rnk = 1