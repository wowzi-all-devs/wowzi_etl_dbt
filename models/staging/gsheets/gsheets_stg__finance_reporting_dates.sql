SELECT 
  Month_Period month_period,
  Year year,
  Month month,
  Reporting_Due_Date reporting_due_date,
  Actual_Reporting_Date actual_reporting_date,
  CASE 
    WHEN DATE(Actual_Reporting_Date) <= DATE(Reporting_Due_Date) THEN 1 
    ELSE 0 
  END AS target
FROM {{ source('staging', 'gsheets_finance_reporting_dates') }}