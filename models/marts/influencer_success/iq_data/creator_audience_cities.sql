{{ config(tags=["iq_data"]) }}
SELECT
  influencer_id,
  channel,
  JSON_EXTRACT_SCALAR(audience_cites, "$.name") AS audience_city_name,
  ROUND(CAST(JSON_EXTRACT_SCALAR(audience_cites, "$.weight")AS numeric) * 100,2) AS audience_city_weight
FROM
  {{ ref("iqdata_stg__creator_profiles") }},
  UNNEST(JSON_EXTRACT_ARRAY(audience_followers_data_audience_geo_cities, "$")) audience_cites