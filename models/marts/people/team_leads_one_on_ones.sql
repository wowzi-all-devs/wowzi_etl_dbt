SELECT  
  Date meeting_date,
  Team team_name,
  Team_Leader team_lead,
  Team_Member team_member
FROM {{ source('staging', 'one_on_one_tracker') }} 
--`bi-staging-1-309112.wowzi_dbt_prod.one_on_one_tracker` 
    WHERE Date is not null