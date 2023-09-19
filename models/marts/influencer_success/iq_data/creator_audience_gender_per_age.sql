{{ config(tags=["iq_data"]) }}
SELECT
    influencer_id,
    channel,
    json_extract_scalar(
        audience_genders_per_age,
        "$.code"
    ) AS audience_age_group,
    ROUND(
        CAST(
            json_extract_scalar(
                audience_genders_per_age,
                "$.male"
            ) AS numeric
        ) * 100,
        2
    ) AS audience_male_percentage,
    ROUND(
        CAST(
            json_extract_scalar(
                audience_genders_per_age,
                "$.female"
            ) AS numeric
        ) * 100,
        2
    ) AS audience_female_percentage,
FROM
    {{ ref("iqdata_stg__creator_profiles") }},
    unnest(
        json_extract_array(
            audience_followers_data_audience_genders_per_age,
            "$"
        )
    ) audience_genders_per_age
