{{ config(tags=["influencer_success"]) }}

SELECT
    influencer_id,
    gender,
    country,
    location,
    age_range,
    job_activity,
    job_eligibility,
    {% for channel in ['FACEBOOK', 'INSTAGRAM', 'TWITTER', 'LINKEDIN', 'TIKTOK'] -%}
       `username_{{ channel }}`,
       `followers_count_{{ channel }}`,
       `influencer_type_{{ channel }}`,
       `channel_status_{{ channel }}`
    {% endfor %}
FROM {{ ref('influencer_facts') }}