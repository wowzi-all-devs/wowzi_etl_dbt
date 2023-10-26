{{ config(
    tags = ["iq_data"]
) }}
SELECT
    influencer_id,
    report_info_report_id AS report_id,
    report_info_created AS report_created_at,
    report_info_profile_updated AS report_updated_at,
    user_profile_username AS username,
    channel,
    user_profile_url AS profile_url,
    user_profile_age_group AS age_group,
    user_profile_followers AS followers_count,
    user_profile_engagement_rate AS engagement_rate,
    user_profile_engagements AS total_profile_engagement,
    user_profile_geo_country_name AS country,
    user_profile_geo_city_name AS city
FROM
    {{ ref("iqdata_stg__creator_profiles") }}
