With metric_targets AS 
(
SELECT 
  Function,
  Dept,
  Metrics,
  Key_Metrics,
  Owner,
  Q1_25,
  Q2_25,
  Q3_25,
  Q4_25,
  row_number() OVER() row_num
FROM `bi-staging-1-309112.wowzi_dbt_prod.gsheets_company_metrics_targets` 
  WHERE Owner <> 'Owner'
  AND Key_Metrics = 'C1'
),

cleaning_1 AS
(
SELECT 
  Function,
  LAST_VALUE(Function IGNORE NULLS) OVER (
      PARTITION BY 1 ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Filled_Function,
  Dept,
  LAST_VALUE(Dept IGNORE NULLS) OVER (
    PARTITION BY 1 ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Filled_Dept,
  Metrics,
  Key_Metrics,
  Owner,
  Q1_25,
  CASE
      -- For cases like "12-17%", extract the first number and append the %
      WHEN REGEXP_CONTAINS(Q1_25, r'\d+-\d+%') THEN 
      REGEXP_EXTRACT(Q1_25, r'^(\d+)-\d+%') || '%'
      -- For cases like "50-55", extract the first number only
      WHEN REGEXP_CONTAINS(Q1_25, r'\d+-\d+') THEN 
      REGEXP_EXTRACT(Q1_25, r'^(\d+)-\d+')
      -- For other cases, return the original value or handle as needed
      ELSE Q1_25
  END AS clean_Q1_25,
  Q2_25,
  CASE
      -- For cases like "12-17%", extract the first number and append the %
      WHEN REGEXP_CONTAINS(Q2_25, r'\d+-\d+%') THEN 
      REGEXP_EXTRACT(Q2_25, r'^(\d+)-\d+%') || '%'
      -- For cases like "50-55", extract the first number only
      WHEN REGEXP_CONTAINS(Q2_25, r'\d+-\d+') THEN 
      REGEXP_EXTRACT(Q2_25, r'^(\d+)-\d+')
      -- For other cases, return the original value or handle as needed
      ELSE Q2_25
  END AS clean_Q2_25,
  Q3_25,
  CASE
      -- For cases like "12-17%", extract the first number and append the %
      WHEN REGEXP_CONTAINS(Q3_25, r'\d+-\d+%') THEN 
      REGEXP_EXTRACT(Q3_25, r'^(\d+)-\d+%') || '%'
      -- For cases like "50-55", extract the first number only
      WHEN REGEXP_CONTAINS(Q3_25, r'\d+-\d+') THEN 
      REGEXP_EXTRACT(Q3_25, r'^(\d+)-\d+')
      -- For other cases, return the original value or handle as needed
      ELSE Q3_25
  END AS clean_Q3_25,
  Q4_25,
  CASE
      -- For cases like "12-17%", extract the first number and append the %
      WHEN REGEXP_CONTAINS(Q4_25, r'\d+-\d+%') THEN 
      REGEXP_EXTRACT(Q4_25, r'^(\d+)-\d+%') || '%'
      -- For cases like "50-55", extract the first number only
      WHEN REGEXP_CONTAINS(Q4_25, r'\d+-\d+') THEN 
      REGEXP_EXTRACT(Q4_25, r'^(\d+)-\d+')
      -- For other cases, return the original value or handle as needed
      ELSE Q4_25
  END AS clean_Q4_25,
  row_num
FROM metric_targets
)

SELECT 
  Filled_Function,
  Filled_Dept,
  Metrics,
  Key_Metrics,
  Owner,
  CASE
      -- Convert percentages (e.g., "10%" -> 0.1)
      WHEN REGEXP_CONTAINS(clean_Q1_25, r'^\d+(\.\d+)?%$') THEN 
      CAST(REPLACE(clean_Q1_25, '%', '') AS NUMERIC) / 100

      -- Convert numbers (e.g. ' $ 2,843,701 ' -> 2843701)
      WHEN clean_Q1_25 LIKE ' $ %,%,% ' OR clean_Q1_25 LIKE ' $ %,%' OR clean_Q1_25 LIKE '%,%'
      THEN CAST(REGEXP_REPLACE(clean_Q1_25, r'[^0-9]', '') AS NUMERIC)

      WHEN clean_Q1_25 = '5th' THEN 3

      ELSE CAST(clean_Q1_25 AS NUMERIC)
  END AS Q1_25,
  CASE
      -- Convert percentages (e.g., "10%" -> 0.1)
      WHEN REGEXP_CONTAINS(clean_Q2_25, r'^\d+(\.\d+)?%$') THEN 
      CAST(REPLACE(clean_Q2_25, '%', '') AS NUMERIC) / 100

      -- Convert numbers (e.g. ' $ 2,843,701 ' -> 2843701)
      WHEN clean_Q2_25 LIKE ' $ %,%,% ' OR clean_Q2_25 LIKE ' $ %,%' OR clean_Q2_25 LIKE '%,%'
      THEN CAST(REGEXP_REPLACE(clean_Q2_25, r'[^0-9]', '') AS NUMERIC)

      WHEN clean_Q2_25 = '5th' THEN 3

      ELSE CAST(clean_Q2_25 AS NUMERIC)
  END AS Q2_25,
  CASE
      -- Convert percentages (e.g., "10%" -> 0.1)
      WHEN REGEXP_CONTAINS(clean_Q3_25, r'^\d+(\.\d+)?%$') THEN 
      CAST(REPLACE(clean_Q3_25, '%', '') AS NUMERIC) / 100

      -- Convert numbers (e.g. ' $ 2,843,701 ' -> 2843701)
      WHEN clean_Q3_25 LIKE ' $ %,%,% ' OR clean_Q3_25 LIKE ' $ %,%' OR clean_Q3_25 LIKE '%,%'
      THEN CAST(REGEXP_REPLACE(clean_Q3_25, r'[^0-9]', '') AS NUMERIC)

      WHEN clean_Q3_25 = '5th' THEN 3

      ELSE CAST(clean_Q3_25 AS NUMERIC)
  END AS Q3_25,
  CASE
      -- Convert percentages (e.g., "10%" -> 0.1)
      WHEN REGEXP_CONTAINS(clean_Q4_25, r'^\d+(\.\d+)?%$') THEN 
      CAST(REPLACE(clean_Q4_25, '%', '') AS NUMERIC) / 100

      -- Convert numbers (e.g. ' $ 2,843,701 ' -> 2843701)
      WHEN clean_Q4_25 LIKE ' $ %,%,% ' OR clean_Q4_25 LIKE ' $ %,%' OR clean_Q4_25 LIKE '%,%'
      THEN CAST(REGEXP_REPLACE(clean_Q4_25, r'[^0-9]', '') AS NUMERIC)

      WHEN clean_Q4_25 = '5th' THEN 3

      ELSE CAST(clean_Q4_25 AS NUMERIC)
  END AS Q4_25
FROM cleaning_1