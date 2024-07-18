WITH contacts AS 

(SELECT * 
  FROM {{ source('hubspot_staging', 'contacts') }}
) 
SELECT
  *
FROM contacts