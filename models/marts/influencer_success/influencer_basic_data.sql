SELECT
    influencer_id,
    influencer_type,
    location,
    country,
    age_range,
    job_eligibility,
    job_activity
FROM {{ ref('influencer_facts') }}