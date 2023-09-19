{{ config(tags=["iq_data"]) }}

SELECT
  influencer_id,
  channel,
  json_extract_scalar(
    audience_gender,
    "$.code"
  ) AS audience_gender,
  ROUND(
    CAST(
      json_extract_scalar(
        audience_gender,
        "$.weight"
      ) AS numeric
    ) * 100,
    2
  ) AS audience_gender_weight,
FROM
  {{ ref("iqdata_stg__creator_profiles") }},
  unnest(
    json_extract_array(
      audience_followers_data_audience_genders,
      "$"
    )
  ) audience_gender
