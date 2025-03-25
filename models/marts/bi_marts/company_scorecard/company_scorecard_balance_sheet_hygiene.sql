SELECT 
  Date,
  Account,
  Comments,
  'targets' data_class,
  1 target
FROM {{ ref('gsheets_stg__balance_sheet_hygiene_jan_2025') }}
UNION ALL 
SELECT 
  Date,
  Account,
  Comments,
  'actuals' data_class,
  CASE 
    WHEN lower(Comments) like '%okay%' THEN 1
    ELSE 0
  END target
FROM {{ ref('gsheets_stg__balance_sheet_hygiene_jan_2025') }}
UNION ALL 
SELECT 
  Date,
  Account,
  Comments,
  'targets' data_class,
  1 target
FROM {{ ref('gsheets_stg__balance_sheet_hygiene_feb_2025') }}
UNION ALL 
SELECT 
  Date,
  Account,
  Comments,
  'actuals' data_class,
  CASE 
    WHEN lower(Comments) like '%okay%' THEN 1
    ELSE 0
  END target
FROM {{ ref('gsheets_stg__balance_sheet_hygiene_feb_2025') }}