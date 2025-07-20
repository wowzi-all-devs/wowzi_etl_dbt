SELECT 
    PARSE_DATE('%B %e %Y', Date) date,
    Platform platform,
    Description description,
    Jira_story jira_story,
    Category category,
    BE_Done be_done,
    FE_Done fe_done,
    case 
         WHEN LOWER(CAST(BE_Done AS STRING)) = 'true' AND LOWER(CAST(FE_Done AS STRING)) = 'n/a' THEN 'complete'
         WHEN LOWER(CAST(BE_Done AS STRING)) = 'n/a' AND LOWER(CAST(FE_Done AS STRING)) = 'true' THEN 'complete'
         WHEN LOWER(CAST(BE_Done AS STRING)) = 'true' AND LOWER(CAST(FE_Done AS STRING)) = 'true' THEN 'complete'
         ELSE 'incomplete' end as status,
    DATE_DIFF(CURRENT_DATE(), DATE_TRUNC(CURRENT_DATE(), QUARTER), WEEK) + 1 week_of_qtr,
    DATE_DIFF(
  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), DATE_TRUNC(CURRENT_DATE(), QUARTER),
  WEEK
) weeks_in_qtr,
      UI_UX_Pass ui_ux_pass
FROM {{ source('staging', 'gsheets__tech_deployment') }} 