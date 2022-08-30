WITH
channel_details AS (
SELECT
    * EXCEPT(id,
      api_id,
      api_key,
      status_backofficer_id,
      status_comment,
      status_update_time)
FROM (
    SELECT
      *,
      {{ influencer_type(followers_count) }} AS influencer_type
    FROM {{ ref('influencer_channel_data') }} )
),
active_influencers AS (
    SELECT 
        influencer_id,
    FROM {{ ref('jobs') }}
    WHERE (invitation_status = "ACCEPTED" or invitation_status = "REJECTED")
        and date_diff(date(current_timestamp()),date(offer_creation_time),MONTH) <= 6
    GROUP BY 1
),
influencer_facts AS (
SELECT *
FROM (
  SELECT
    inf.influencer_id,
    date_account_created,
    first_name,
    last_name,
    personal_id_number,
    email,
    email_verified,
    mobile_number,
    mobile_number_verified,
    gender,
    country,
    location,
    dob,
    income_category,
    income_currency,
    smileidentity_status,
    total_campaigns,
    total_net_payment,
    first_campaign_date,
    last_campaign_date,
    offered_jobs,
    total_accepted,
    total_expired,
    total_not_accepted,
    username,
    channel,
    followers_count,
    jobs_last_week,
    jobs_last_2_weeks,
    jobs_last_4_weeks,
    jobs_last_month,
    jobs_last_2_months,
    jobs_last_3_months,
    jobs_last_6_months,
    status AS channel_status,
    CASE
        WHEN DATE_DIFF(current_date, dob, year) <18 THEN 'Under 18'
        WHEN DATE_DIFF(current_date, dob, year) BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATE_DIFF(current_date, dob, year) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATE_DIFF(current_date, dob, year) BETWEEN 36 AND 45 THEN '36-45'
        WHEN DATE_DIFF(current_date, dob, year) IS NULL THEN 'No DOB'
        ELSE '>45'
    END
    AS age_range,
    CASE 
        WHEN inf.influencer_id IN (select * from active_influencers) THEN 'Active' ELSE 'Inactive'
    END
    AS job_activity,
    CASE 
        WHEN smileidentity_status='APPROVED'
        AND DATE_DIFF(CURRENT_DATE(), inf.dob, year) >= 18
        AND inf.location IS NOT NULL
        AND inf.country IS NOT NULL
        AND inf.gender IS NOT NULL
        AND inf.dob IS NOT NULL
        AND inf.income_category IS NOT NULL
        AND inf.influencer_id in (select influencer_id from {{ ref('influencer_channel_data') }} where status='APPROVED' and followers_count>=251)
       then 'Eligible' else 'Ineligible'
    END 
    AS job_eligibility,
    cd.influencer_type
  FROM
    {{ ref('influencers') }} inf
  left JOIN
    channel_details cd
  ON
    cd.influencer_id=inf.influencer_id) PIVOT(MAX(username) AS username,
    MAX(followers_count) AS followers_count,
    MAX(influencer_type) as influencer_type,
    MAX(channel_status) AS channel_status FOR channel IN ("INSTAGRAM","FACEBOOK","TWITTER","LINKEDIN","TIKTOK"))
)
select *
from influencer_facts