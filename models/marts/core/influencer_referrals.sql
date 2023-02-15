WITH payments AS (
SELECT 
    influencer_id,
    sum(amount_usd) as amount_usd
FROM {{ ref('influencer_payouts') }}
    WHERE lower(payment_status) in ('successful', 'manual', 'new')
    GROUP BY influencer_id
),

influencer_referrals AS (
SELECT
    ref.influencer_id,
    inf.first_name || ' ' || inf.last_name as influencer_name,
    (CASE WHEN inf.gender IS NULL THEN 'GENDER NOT SET'
    ELSE inf.gender END) AS gender,
    inf.job_eligibility,
    (CASE WHEN inf.country IS NULL THEN 'COUNTRY NOT SET'
    ELSE inf.country END) AS country,
    inf.offered_jobs AS total_offered_jobs,
    (CASE WHEN inf.offered_jobs > 0 then 'YES'
    ELSE 'NO' END) AS inf_offered_job,
    inf.total_accepted AS total_accepted_jobs,
    (CASE WHEN inf.total_accepted > 0 then 'YES'
    ELSE 'NO' END) AS inf_accepted_job,
    p.amount_usd,
    (CASE WHEN p.amount_usd > 0 then 'YES'
    ELSE 'NO' END) AS inf_completed_job,
    inf.job_activity,
    ref.referred_by_influencer_id as ambassador_influencer_id,
    inf2.first_name || ' ' || inf2.last_name as ambassador_name,
    inf2.job_eligibility as ambassador_job_eligibility,
    inf2.country as ambassador_country,
    ref.creation_time
FROM {{ ref('postgres_stg__influencer_referral_influencer') }} ref
LEFT JOIN {{ ref('influencer_facts') }} inf on ref.influencer_id = inf.influencer_id
LEFT JOIN {{ ref('influencer_facts') }} inf2 on ref.referred_by_influencer_id = inf2.influencer_id
LEFT JOIN payments p on ref.influencer_id = p.influencer_id
)

SELECT * FROM influencer_referrals
