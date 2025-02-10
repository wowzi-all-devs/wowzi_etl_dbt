SELECT 
  Distinct
  CAST(Id AS INT64) Id,
  CAST(DocNumber AS STRING) DocNumber,
  CAST(TxnDate AS DATE) TxnDate,
  CAST(CustomerRefName AS STRING) CustomerRefName,
  CAST(CurrencyRefName AS STRING) CurrencyRefName,
  CAST(ExchangeRate AS NUMERIC) ExchangeRate,
  CAST(TotalAmt AS NUMERIC) TotalAmt,
  CAST(Balance AS NUMERIC) Balance,
  case when payment_id is null then 'Not Paid'
  else 'Paid' end payment_status,
  CAST(payment_id AS STRING) payment_id,
  CAST(payment_txndate AS DATE) payment_txndate,
  'actuals' data_class,
  CAST(NULL AS STRING) target_metric,
  CAST(NULL AS STRING) function,
  CAST(NULL AS STRING) dept_name,
  CAST(NULL AS STRING) metric_name,
  CAST(SalesTermRefName AS STRING) segment,
  CAST(payment_txndate AS DATE) Date,
  CAST(NULL AS STRING) target_type,
  DATE_DIFF(DATE(payment_txndate), DATE(TxnDate), DAY) AS target
FROM `bi-staging-1-309112.wowzi_dbt_prod.finance_invoice_data` 
where Balance = 0
UNION ALL
SELECT 
  Distinct
  CAST(NULL AS INT64) Id,
  CAST(NULL AS STRING) DocNumber,
  CAST(NULL AS DATE) TxnDate,
  CAST(NULL AS STRING) CustomerRefName,
  CAST(NULL AS STRING) CurrencyRefName,
  CAST(NULL AS NUMERIC) ExchangeRate,
  CAST(NULL AS NUMERIC) TotalAmt,
  CAST(NULL AS NUMERIC) Balance,
  CAST(NULL AS STRING)  payment_status,
  CAST(NULL AS STRING) payment_id,
  CAST(NULL AS DATE) payment_txndate,
  'targets' data_class,
  'AR' target_metric,
  function,
  dept_name,
  metric_name,
  segment,
  CAST(Date AS DATE) Date,
  CAST(target_type AS STRING) target_type,
  target
FROM `bi-staging-1-309112.wowzi_dbt_prod.accounts_receivable_scorecard_targets` 