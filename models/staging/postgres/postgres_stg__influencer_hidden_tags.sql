SELECT  
  influencer_id,
  hidden_tag,
  created_at
FROM {{ source('staging', 'influencer_hidden_tags') }}