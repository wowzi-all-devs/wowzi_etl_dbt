SELECT 
  Month_Period month_period,
  Year year,
  Month month,
  DATE(Reporting_Due_Date) AS reporting_due_date,
  DATE(Actual_Reporting_Date) AS actual_reporting_date,
  CASE
    WHEN DATE(Actual_Reporting_Date) <= DATE(Reporting_Due_Date) + 2 THEN 1
    ELSE 0
  END AS target
FROM {{ source('staging', 'gsheets_finance_reporting_dates') }}