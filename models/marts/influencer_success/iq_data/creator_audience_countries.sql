{{ config(tags=["iq_data"]) }}
SELECT
  influencer_id,
  channel,
  JSON_EXTRACT_SCALAR(audience_countries, "$.name") AS audience_country_name,
  ROUND(CAST(JSON_EXTRACT_SCALAR(audience_countries, "$.weight")AS numeric) * 100,2) AS audience_country_weight
FROM
  {{ ref("iqdata_stg__creator_profiles") }},
  UNNEST(JSON_EXTRACT_ARRAY(audience_followers_data_audience_geo_countries, "$")) audience_countries