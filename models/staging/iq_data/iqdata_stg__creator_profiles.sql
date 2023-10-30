{{ config(
    tags = ["iq_data"]
) }}
WITH influencer_profiles AS (
  SELECT
    DISTINCT report_info_report_id,
    report_info_created,
    report_info_profile_updated,
    user_profile_username,
    user_profile_url,
    user_profile_fullname,
    user_profile_gender,
    user_profile_age_group,
    user_profile_followers,
    user_profile_engagement_rate,
    user_profile_engagements,
    user_profile_geo_country_name,
    user_profile_geo_city_name,
    user_profile_avg_views,
    user_profile_avg_comments,
    null as user_profile_avg_shares,
    null as user_profile_avg_saves,
    null as user_profile_total_likes,
    audience_followers_data_audience_credibility,
    audience_followers_data_audience_genders,
    audience_followers_data_audience_ages,
    audience_followers_data_audience_languages,
    audience_followers_data_audience_geo_countries,
    audience_followers_data_audience_geo_cities,
    audience_followers_data_audience_interests,
    audience_followers_data_audience_genders_per_age
  FROM
    {{ source(
      "iq_data",
      "instagram_iq_data_profiles"
    ) }}
  UNION
    DISTINCT
  SELECT
    report_info_report_id,
    CAST(
      report_info_created AS TIMESTAMP
    ) AS report_info_created,
    CAST(
      report_info_profile_updated AS TIMESTAMP
    ) AS report_info_profile_updated,
    user_profile_username,
    user_profile_url,
    user_profile_fullname,
    user_profile_gender,
    user_profile_age_group,
    user_profile_followers,
    user_profile_engagement_rate,
    user_profile_engagements,
    user_profile_geo_country_name,
    user_profile_geo_city_name,
    user_profile_avg_views,
    user_profile_avg_comments,
    user_profile_avg_shares,
    user_profile_avg_saves,
    user_profile_total_likes,
    audience_followers_data_audience_credibility,
    audience_followers_data_audience_genders,
    audience_followers_data_audience_ages,
    audience_followers_data_audience_languages,
    audience_followers_data_audience_geo_countries,
    audience_followers_data_audience_geo_cities,
    audience_followers_data_audience_interests,
    audience_followers_data_audience_genders_per_age
  FROM
    {{ source(
      "iq_data",
      "instagram_iq_data_profiles_2"
    ) }}
)
SELECT
  x.*,
  cd.influencer_id
FROM
  (
    SELECT
      report_info_report_id,
      report_info_created,
      report_info_profile_updated,
      t.user_profile_username,
      user_profile_url,
      user_profile_type,
      user_profile_fullname,
      user_profile_gender,
      user_profile_age_group,
      ifnull(user_profile_followers,0) as user_profile_followers,
      ifnull(user_profile_engagement_rate,0) as user_profile_engagement_rate,
      ifnull(user_profile_engagements,0) as user_profile_engagements,
      user_profile_geo_country_name,
      user_profile_geo_city_name,
      ifnull(user_profile_avg_views,0) as user_profile_avg_views,
      ifnull(user_profile_avg_comments,0) as user_profile_avg_comments,
      ifnull(user_profile_avg_shares,0) as user_profile_avg_shares,
      ifnull(user_profile_avg_saves,0) as user_profile_avg_saves,
      ifnull(user_profile_total_likes,0) as user_profile_total_likes,
      audience_followers_data_audience_credibility,
      audience_followers_data_audience_genders,
      audience_followers_data_audience_ages,
      audience_followers_data_audience_languages,
      audience_followers_data_audience_geo_countries,
      audience_followers_data_audience_geo_cities,
      audience_followers_data_audience_interests,
      audience_followers_data_audience_genders_per_age,
      CASE
        WHEN user_profile_url LIKE "%instagram%" THEN 'INSTAGRAM'
        WHEN user_profile_url LIKE "%tiktok%" THEN 'TIKTOK'
      END AS channel
    FROM
      influencer_profiles t
      INNER JOIN (
        SELECT
          user_profile_username AS username,
          MAX(report_info_created) max_date
        FROM
          influencer_profiles
        GROUP BY
          1
      ) bd2
      ON t.user_profile_username = bd2.username
      AND t.report_info_created = bd2.max_date
  ) x
  INNER JOIN {{ ref("postgres_stg__influencer_channel_data") }}
  cd
  ON cd.channel = x.channel
  AND LOWER(
    cd.username
  ) = x.user_profile_username
