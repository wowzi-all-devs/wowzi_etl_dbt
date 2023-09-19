{{ config(tags=["iq_data"]) }}

SELECT
  influencer_id,
  channel,
  json_extract_scalar(
    audience_ages,
    "$.code"
  ) AS audience_ages,
  ROUND(
    CAST(json_extract_scalar(audience_ages, "$.weight") AS numeric) * 100,
    2
  ) AS audience_ages_weight,
FROM
  {{ ref("iqdata_stg__creator_profiles") }},
  unnest(
    json_extract_array(
      audience_followers_data_audience_ages,
      "$"
    )
  ) audience_ages
