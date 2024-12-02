WITH payments AS (
SELECT 
    influencer_id,
    sum(amount_usd) AS amount_usd
FROM {{ ref('influencer_payouts') }}
    WHERE lower(payment_status) in ('successful', 'manual', 'new', 'completed')
    GROUP BY influencer_id
),

influencer_referrals AS (
SELECT
    ref.influencer_id,
    inf.first_name||' '||inf.last_name AS influencer_name,
    inf.date_account_created,
    inf.email,
    inf.email_verified,
    inf.mobile_number,
    inf.mobile_number_verified,
    (CASE WHEN inf.gender IS NULL THEN 'GENDER NOT SET'
        ELSE inf.gender END) AS gender,
    inf.age_range,
    (CASE WHEN inf.country IS NULL THEN 'COUNTRY NOT SET'
        ELSE inf.country END) AS country,
    inf.location,
    inf.username_FACEBOOK,
    inf.channel_status_FACEBOOK,
    inf.followers_count_FACEBOOK,
    inf.username_INSTAGRAM,
    inf.channel_status_INSTAGRAM,
    inf.followers_count_INSTAGRAM,
    inf.username_TWITTER,
    inf.channel_status_TWITTER,
    inf.followers_count_TWITTER,
    inf.username_TIKTOK,
    inf.channel_status_TIKTOK,
    inf.followers_count_TIKTOK,
    inf.username_LINKEDIN,
    inf.channel_status_LINKEDIN,
    inf.followers_count_LINKEDIN,
    (CASE 
    WHEN 'APPROVED' IN (inf.channel_status_FACEBOOK,inf.channel_status_INSTAGRAM,inf.channel_status_TWITTER,inf.channel_status_TIKTOK,inf.channel_status_LINKEDIN) THEN true
    ELSE false END) AS atleast_one_approved_platform,
    inf.smileidentity_status,
    inf.job_eligibility,
    inf.job_activity,
    inf.offered_jobs AS total_offered_jobs,
    (CASE WHEN inf.offered_jobs > 0 then true
    ELSE false END) AS inf_offered_job,
    inf.total_accepted AS total_accepted_jobs,
    (CASE WHEN inf.total_accepted > 0 then true
        ELSE false END) AS inf_accepted_job,
    p.amount_usd amount_usd_earned_by_referred_inf,
    ref.referred_by_influencer_id AS ambassador_influencer_id,
    ref_codes.code AS referral_code,
    inf2.first_name||' '||inf2.last_name AS ambassador_influencer_name,
    (CASE WHEN inf2.gender IS NULL THEN 'GENDER NOT SET'
        ELSE inf2.gender END) AS ambassador_gender,
    inf2.age_range ambassador_age_range,
    (CASE WHEN inf2.country IS NULL THEN 'COUNTRY NOT SET'
        ELSE inf2.country END) AS ambassador_country,
    inf2.location ambassador_location,
    inf2.job_eligibility ambassador_job_eligibility,
    inf2.job_activity ambassador_job_activity,
    inf2.offered_jobs AS ambassador_total_offered_jobs,
    (CASE WHEN inf2.offered_jobs > 0 then true
    ELSE false END) AS ambassador_offered_job,
    inf2.total_accepted AS ambassador_total_accepted_jobs,
    (CASE WHEN inf2.total_accepted > 0 then true
        ELSE false END) AS ambassador_accepted_job,
    p2.amount_usd amount_usd_earned_by_ambassador,
    ref.creation_time
FROM {{ ref('postgres_stg__influencer_referral_influencer') }} ref
LEFT JOIN {{ ref('influencer_referral_code') }} ref_codes ON ref.referred_by_influencer_id = ref_codes.influencer_id
LEFT JOIN {{ ref('influencer_facts') }} inf on ref.influencer_id = inf.influencer_id
LEFT JOIN {{ ref('influencer_facts') }} inf2 on ref.referred_by_influencer_id = inf2.influencer_id
LEFT JOIN payments p on ref.influencer_id = p.influencer_id
LEFT JOIN payments p2 on ref.referred_by_influencer_id = p2.influencer_id
)

SELECT 
    influencer_id,
    influencer_name,
    date_account_created,
    email,
    email_verified,
    mobile_number,
    mobile_number_verified,
    gender,
    age_range,
    country,
    location,
    username_FACEBOOK,
    channel_status_FACEBOOK,
    followers_count_FACEBOOK,
    username_INSTAGRAM,
    channel_status_INSTAGRAM,
    followers_count_INSTAGRAM,
    username_TWITTER,
    channel_status_TWITTER,
    followers_count_TWITTER,
    username_TIKTOK,
    channel_status_TIKTOK,
    followers_count_TIKTOK,
    username_LINKEDIN,
    channel_status_LINKEDIN,
    followers_count_LINKEDIN,
    atleast_one_approved_platform,
    smileidentity_status,
    job_eligibility,
    job_activity,
    total_offered_jobs,
    inf_offered_job,
    total_accepted_jobs,
    inf_accepted_job,
    amount_usd_earned_by_referred_inf,
    ambassador_influencer_id,
    referral_code,
    ambassador_influencer_name,
    ambassador_gender,
    ambassador_age_range,
    ambassador_country,
    ambassador_location,
    ambassador_job_eligibility,
    ambassador_job_activity,
    ambassador_total_offered_jobs,
    ambassador_offered_job,
    ambassador_total_accepted_jobs,
    ambassador_accepted_job,
    amount_usd_earned_by_ambassador,
    creation_time
FROM influencer_referrals
