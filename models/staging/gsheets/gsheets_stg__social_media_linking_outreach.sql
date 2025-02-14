SELECT 
  S_N row_num, 
  Creator_ID creator_id,
  Creator_Name creator_name,
  Outreach_Status outreach_status
FROM {{ source('staging', 'gsheets_social_media_linking_outreach') }}