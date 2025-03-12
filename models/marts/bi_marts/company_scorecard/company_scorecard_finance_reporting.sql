SELECT 
    month_period,
    month_period Date,
    DATE(reporting_due_date) reporting_due_date,
    DATE(actual_reporting_date) actual_reporting_date,
    target,
    NULL function,
    NULL dept_name,
    NULL metric_name,
    'actuals' data_class
FROM `bi-staging-1-309112.wowzi_dbt_prod.gsheets_stg__finance_reporting_dates` 
UNION ALL 
SELECT 
    month_period,
    month_period Date,
    NULL reporting_due_date,
    NULL actual_reporting_date,
    target,
    function,
    dept_name,
    metric_name,
    'targets' data_class
FROM `bi-staging-1-309112.wowzi_dbt_prod.finance_reporting_scorecard_targets` 