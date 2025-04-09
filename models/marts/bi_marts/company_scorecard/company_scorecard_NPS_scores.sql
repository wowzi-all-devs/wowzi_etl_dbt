/* Targets */
select 
    Function function,
    dept_name, 
    metric_name,
    segment,
    Date txndate,
    'targets' data_class,
    Target value
FROM bi-staging-1-309112.wowzi_dbt_prod.company_scorecard_NPS_targets 
 
 UNION ALL

/* Actuals */
select 
    CAST(NULL AS STRING) function,
    CAST(NULL AS STRING) dept_name,
    CAST(NULL AS STRING) metric_name,
    CAST(NULL AS STRING) segment,
    date(Date) txndate,
    'actuals' data_class,
    nps_score value
FROM 
     {{ ref('gsheets__stg__creator_nps_march') }}