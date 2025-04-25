SELECT 
  Id,
  VendorRefName,
  DepartmentRefName,
  APAccountRefName,
  TxnDate,
  DueDate,
  CurrencyRefName,
  ExchangeRate,
  TotalAmt,
  Balance,
  -- PrivateNote,
  billpayment_txn_date,
  case when billpayment_txn_date is null then 'Not Paid'
  else 'Paid' end payment_status,
  CAST(billpayment_txn_date AS DATE) Date,
  DATE_DIFF(DATE(billpayment_txn_date), DATE(DueDate), DAY) AS target,
  'actuals' data_class,
  CAST(NULL AS STRING) target_metric,
  CAST(NULL AS STRING) target_type,
  NULL function,
  NULL dept_name,
  NULL metric_name
FROM `bi-staging-1-309112.wowzi_dbt_prod.finance_bills_and_payments` 
  WHERE Balance = 0
UNION ALL 
SELECT 
    NULL Id,
    NULL VendorRefName,
    NULL DepartmentRefName,
    NULL APAccountRefName,
    NULL TxnDate,
    NULL DueDate,
    NULL CurrencyRefName,
    NULL ExchangeRate,
    NULL TotalAmt,
    NULL Balance,
    -- NULL PrivateNote,
    NULL billpayment_txn_date,
    NULL  payment_status,
    Date,
    target,
    'targets' data_class,
    'AP' target_metric,
    target_type,
    function,
    dept_name,
    metric_name
FROM `bi-staging-1-309112.wowzi_dbt_prod.accounts_payable_scorecard_targets`