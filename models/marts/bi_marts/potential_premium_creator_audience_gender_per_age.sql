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
    ) AS audience_female_percentage
FROM `bi-staging-1-309112.wowzi_dbt_prod.potential_premium_creator_iqdata` c ,
unnest(
    json_extract_array(
      audience_followers_data_audience_genders_per_age,
      "$"
    )
  ) audience_genders_per_age
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f
ON lower(c.user_profile_username) = lower(f.username_INSTAGRAM)
LEFT JOIN `bi-staging-1-309112.wowzi_dbt_prod.influencer_facts` f2
ON lower(c.user_profile_username) = lower(f2.username_TIKTOK)