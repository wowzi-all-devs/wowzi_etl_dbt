{{ config(
    tags = ["iq_data"]
) }}

SELECT
    influencer_id,
    channel,
    json_extract_scalar(
        audience_languages,
        "$.name"
    ) AS audience_language,
    ROUND(
        CAST(
            json_extract_scalar(
                audience_languages,
                "$.weight"
            ) AS numeric
        ) * 100,
        2
    ) AS audience_language_weight,
FROM
    {{ ref("iqdata_stg__creator_profiles") }},
    unnest(
        json_extract_array(
            audience_followers_data_audience_languages,
            "$"
        )
    ) audience_languages
