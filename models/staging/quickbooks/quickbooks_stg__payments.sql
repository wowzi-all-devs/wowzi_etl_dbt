SELECT 
  DISTINCT
  Id,
  --CustomerRef,
  JSON_VALUE(CustomerRef, '$.value') AS CustomerRefValue,
  JSON_VALUE(CustomerRef, '$.name') AS CustomerRefName,
  --DepositToAccountRef,
  JSON_VALUE(DepositToAccountRef, '$.value') AS DepositToAccountRefValue,
  TxnDate,
  --CurrencyRef,
  JSON_VALUE(CurrencyRef, '$.value') AS CurrencyRefValue,
  JSON_VALUE(CurrencyRef, '$.name') AS CurrencyRefName,
  ExchangeRate,
  TotalAmt,
  UnappliedAmt,
  ProcessPayment,
  json_extract_scalar(PaymentMethodRef, '$.value') PaymentMethodRef,
  PaymentRefNum,
  PrivateNote,
  --MetaData,
  JSON_VALUE(MetaData, '$.CreateTime') AS CreateTime,
  JSON_VALUE(MetaData, '$.LastUpdatedTime') AS LastUpdatedTime,
  -- Line,
  -- Extract top-level fields
  JSON_VALUE(Line, '$[0].Amount') AS Line_Amount,

  -- Extract Linked Transaction details
  JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnId') AS Line_TxnId,
  JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnType') AS Line_TxnType,

  -- Extract values from LineEx.any array
  JSON_VALUE(Line, '$[0].LineEx.any[0].value.Name') AS Line_TxnId_Name,
  JSON_VALUE(Line, '$[0].LineEx.any[0].value.Value') AS Line_TxnId_Value
FROM {{ source('staging', 'payments') }}








/**
SELECT 
  id,
  Line,
  JSON_EXTRACT_SCALAR(Line, '$[0].Amount') AS line_amount,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].LinkedTxn'), '$[0].TxnId') AS linkedtxn_txid,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].LinkedTxn'), '$[0].TxnType') AS linkedtxn_txntype,
  domain,
  sparse,
  TxnDate,
  MetaData,
  JSON_EXTRACT(MetaData, '$.CreateTime') AS create_time,
  JSON_EXTRACT(MetaData, '$.LastUpdatedTime') AS lastupdate_time,
  TotalAmt,
  LinkedTxn,
  SyncToken,
  CurrencyRef,
  JSON_EXTRACT(CurrencyRef, '$.name') AS currency_name,
  JSON_EXTRACT(CurrencyRef, '$.value') AS currency,
  CustomerRef,
  JSON_EXTRACT(CustomerRef, '$.name') AS customer_name,
  PrivateNote,
  ARAccountRef,
  ExchangeRate,
  UnappliedAmt,
  PaymentRefNum,
  ProcessPayment,
  PaymentMethodRef,
  DepositToAccountRef
FROM {{ source('staging', 'payments') }}
**/