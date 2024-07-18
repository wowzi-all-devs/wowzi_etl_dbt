WITH companies AS
(SELECT  
  *
FROM {{ source('hubspot_staging', 'companies') }}
)

SELECT
  *
FROM companies
