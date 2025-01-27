SELECT 
  Feature feature_name,
  Feature_Type feature_type,
  Status status,
  Release_Date actual_release_date
FROM {{ source('staging', 'gsheets_tech_deployments') }}