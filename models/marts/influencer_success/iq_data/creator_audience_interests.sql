{{ config(
    tags = ["iq_data"]
) }}

SELECT
    influencer_id,
    channel,
    json_extract_scalar(
        audience_interests,
        "$.name"
    ) AS audience_interests,
    CAST(
        json_extract_scalar(
            audience_interests,
            "$.affinity"
        ) AS numeric
    ) AS audience_interests_affinity,
FROM
    {{ ref("iqdata_stg__creator_profiles") }},
    unnest(
        json_extract_array(
            audience_followers_data_audience_interests,
            "$"
        )
    ) audience_interests
