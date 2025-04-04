select 
    date(Date) date,
    On_a_scale_of_0_to_10__how_likely_are_you_to_recommend_Wowzi_to_a_friend_or_colleague___0___Not_at_all_likely__10___Extremely_likely_
    as nps_score
FROM 
    {{ source('staging', 'gsheets__creator_nps_march') }}

