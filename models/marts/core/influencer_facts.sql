WITH
channel_details AS (
  SELECT
    *,
    CASE
      WHEN followers_count IS NULL THEN 'No Followers Reported'
      WHEN followers_count < 250 THEN 'NONE'
      WHEN followers_count < 5000 THEN 'NANO'
      WHEN followers_count < 30000 THEN 'MICRO'
      WHEN followers_count < 100000 THEN 'MACRO'
      WHEN followers_count < 500000 THEN 'MEGA'
      ELSE 'META'
    END AS influencer_type
  FROM {{ ref('postgres_stg__influencer_channel_data') }}
),
active_influencers AS (
  SELECT DISTINCT influencer_id
  FROM {{ ref('postgres_stg__jobs') }}
  WHERE invitation_status IN ('ACCEPTED','REJECTED')
    AND DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(offer_creation_time), MONTH) <= 6
),
influencers 
AS (
  SELECT *
  FROM (
    SELECT
      inf.influencer_id,
      date_account_created,
      INITCAP(first_name) AS first_name,
      INITCAP(last_name)  AS last_name,
      personal_id_number,
      email,
      email_verified,
      mobile_number,
      mobile_number_verified,
      INITCAP(gender) AS gender,
      COALESCE(country, 'KE') AS country,
      INITCAP(location) AS location,
      dob,
      income_category,
      income_currency,
      INITCAP(smileidentity_status) AS smileidentity_status,
      total_campaigns,
      total_net_payment,
      first_campaign_date,
      last_campaign_date,
      offered_jobs,
      total_accepted,
      total_expired,
      total_not_accepted,

      cd.username,
      cd.channel,
      cd.followers_count,
      cd.status AS channel_status,
      cd.influencer_type,

      jobs_last_week,
      jobs_last_2_weeks,
      jobs_last_4_weeks,
      jobs_last_month,
      jobs_last_2_months,
      jobs_last_3_months,
      jobs_last_6_months,

      DATE_DIFF(CURRENT_DATE(), DATE(dob), YEAR) AS age,
      CASE
        WHEN dob IS NULL THEN 'No DOB'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(dob), YEAR) < 18 THEN 'Under 18'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(dob), YEAR) BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(dob), YEAR) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(dob), YEAR) BETWEEN 36 AND 45 THEN '36-45'
        ELSE '>45'
      END AS age_range,

      inf.tax_external_id,
      IF(ai.influencer_id IS NOT NULL, TRUE, FALSE) AS job_activity
    FROM {{ ref('dim_influencers') }} inf
    LEFT JOIN channel_details cd
      ON cd.influencer_id = inf.influencer_id
    LEFT JOIN active_influencers ai
      ON ai.influencer_id = inf.influencer_id
  )
  PIVOT (
    MAX(username)        AS username,
    MAX(followers_count) AS followers_count,
    MAX(influencer_type) AS influencer_type,
    MAX(channel_status)  AS channel_status
    FOR channel IN ('INSTAGRAM','FACEBOOK','TWITTER','LINKEDIN','TIKTOK')
  )
 ),
fixed AS (
  SELECT
    -- i.* 
    /*if the last name is blank and first name has more then one name, 
      then the last name is the last word in the first name*/
    *
      REPLACE (
      -- new first_name
      CASE WHEN REGEXP_CONTAINS(TRIM(first_name), r'\s+')
           THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(first_name), r'\s+', ' '), r'\s+\S+$', '')
           ELSE TRIM(first_name)
      END AS first_name,
      -- new last_name
      CASE WHEN REGEXP_CONTAINS(TRIM(first_name), r'\s+')
           THEN REGEXP_EXTRACT(TRIM(first_name), r'(\S+)$')
           ELSE last_name
      END AS last_name
      -- -- new country
      -- c.Country as country
    )
  FROM influencers  
    -- left join 
    -- bi-staging-1-309112.wowzi_dbt_prod.country_key c 
    -- on i.country = c.Key
),
influencer_facts AS (
  SELECT
    influencer_id,
    date_account_created, first_name, last_name,
    personal_id_number, email, email_verified, mobile_number,
    mobile_number_verified, gender, country, location, dob,
    income_category, income_currency, smileidentity_status,
  CASE 
      WHEN lower(smileidentity_status) ='approved'
      AND DATE_DIFF(CURRENT_DATE(), fixed.dob, year) >= 18
      AND fixed.location IS NOT NULL
      AND fixed.country IS NOT NULL
      AND fixed.gender IS NOT NULL
      AND fixed.dob IS NOT NULL
      AND fixed.income_category IS NOT NULL
      /*inf has at least one approved SM account (not Linkedin) that has over 250 ffs*/
      AND fixed.influencer_id in 
      (
        select distinct
        influencer_id from 
        `bi-staging-1-309112.wowzi_dbt_prod.postgres_stg__influencer_channel_data` 
        where lower(status) ='approved' and followers_count>=251 and lower(channel) <> 'linkedin'
      )
       then True else False
     END 
    AS job_eligibility,
    job_activity, total_campaigns, total_net_payment, first_campaign_date,
    last_campaign_date, offered_jobs, total_accepted, total_expired,
    total_not_accepted, jobs_last_week, jobs_last_2_weeks,
    jobs_last_4_weeks, jobs_last_month, jobs_last_2_months, jobs_last_3_months,
    jobs_last_6_months, age, age_range, tax_external_id, username_INSTAGRAM,
    followers_count_INSTAGRAM, influencer_type_INSTAGRAM, channel_status_INSTAGRAM, username_FACEBOOK,
    followers_count_FACEBOOK, influencer_type_FACEBOOK, channel_status_FACEBOOK, username_TWITTER,
    followers_count_TWITTER, influencer_type_TWITTER, channel_status_TWITTER, username_LINKEDIN,
    followers_count_LINKEDIN, influencer_type_LINKEDIN, channel_status_LINKEDIN,
    username_TIKTOK, followers_count_TIKTOK, influencer_type_TIKTOK, channel_status_TIKTOK
  FROM fixed
)
SELECT * FROM influencer_facts
