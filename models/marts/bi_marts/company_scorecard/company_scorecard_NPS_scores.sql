/* Targets */
select 
    Function function,
    dept_name, 
    metric_name,
    segment,
    Date txndate,
    'targets' data_class,
    Target value,
    CAST(NULL AS STRING) category
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
    nps_score value,
    case 
        when nps_score >= 9 then 'Promoter'
        when nps_score >= 7 and nps_score <= 8 then 'Passive'
        when nps_score <= 6 then 'Detractor'
    end as category
FROM 
     {{ ref('gsheets__stg__creator_nps_march') }}