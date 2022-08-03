SELECT
    influencer_id,
    gender,
    country,
    location,
    age_range,
    job_activity,
    job_eligibility,
    influencer_type
FROM {{ ref('influencer_facts') }}