SELECT 
  DISTINCT
  Id,
  Name,
  type,
  Active,
  DueDays,
  --MetaData,
  JSON_VALUE(MetaData, '$.CreateTime') AS CreateTime,
  JSON_VALUE(MetaData, '$.LastUpdatedTime') AS LastUpdatedTime
FROM {{ source('staging', 'terms') }}