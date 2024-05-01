WITH creator_audience_countries AS
(
SELECT 
    distinct
    user_profile_user_id,
    user_profile_type,
    user_profile_username,
    (CASE 
        WHEN c.user_profile_type = 'instagram' then f.influencer_id
        WHEN c.user_profile_type = 'tiktok' then f2.influencer_id
        ELSE NULL 
    END) AS influencer_id,
    JSON_EXTRACT_SCALAR(audience_countries, "$.name") AS audience_country_name,
  ROUND(CAST(JSON_EXTRACT_SCALAR(audience_countries, "$.weight")AS numeric) * 100,2) AS audience_country_weight
FROM `bi-staging-1-309112.wowzi_dbt_prod.potential_premium_creator_iqdata` c ,
unnest(
    json_extract_array(
      audience_followers_data_audience_geo_countries,
      "$"
    )
  ) audience_countries
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
ON lower(c.user_profile_username) = lower(f.username_INSTAGRAM)
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f2
ON lower(c.user_profile_username) = lower(f2.username_TIKTOK)
)

SELECT
    user_profile_user_id,
    user_profile_type,
    user_profile_username,
    influencer_id,
    audience_country_name,
    audience_country_weight
FROM creator_audience_countries