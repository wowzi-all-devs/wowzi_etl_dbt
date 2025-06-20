SELECT 
  DISTINCT
  Id,
  
  -- Customer & Account Info
  JSON_VALUE(CustomerRef, '$.value') AS CustomerRefValue,
  JSON_VALUE(CustomerRef, '$.name') AS CustomerRefName,
  JSON_VALUE(DepositToAccountRef, '$.value') AS DepositToAccountRefValue,
  
  -- Dates & Currency
  TxnDate,
  JSON_VALUE(CurrencyRef, '$.value') AS CurrencyRefValue,
  JSON_VALUE(CurrencyRef, '$.name') AS CurrencyRefName,
  ExchangeRate,
  
  -- Amounts & Notes
  TotalAmt,
  UnappliedAmt,
  ProcessPayment,
  JSON_EXTRACT_SCALAR(PaymentMethodRef, '$.value') AS PaymentMethodRef,
  PaymentRefNum,
  PrivateNote,
  
  -- Metadata
  JSON_VALUE(MetaData, '$.CreateTime') AS CreateTime,
  JSON_VALUE(MetaData, '$.LastUpdatedTime') AS LastUpdatedTime,

  -- Extract top-level Line data
  JSON_VALUE(Line, '$[0].Amount') AS Line_Amount,

  -- Extract LinkedTxn info from Line
  JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnId') AS Line_TxnId,
  JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnType') AS Line_TxnType,

  -- Extract sample LineEx.any values (debugging)
  JSON_VALUE(Line, '$[0].LineEx.any[0].value.Name') AS Line_TxnId_Name,
  JSON_VALUE(Line, '$[0].LineEx.any[0].value.Value') AS Line_TxnId_Value,

    (
    SELECT JSON_VALUE(any_entry, '$.value.Value')
    FROM UNNEST(JSON_EXTRACT_ARRAY(Line)) AS line_item,
         UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(line_item, '$.LineEx.any'))) AS any_entry
    WHERE JSON_VALUE(any_entry, '$.value.Name') = 'txnReferenceNumber'
      AND EXISTS (
        SELECT 1
        FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(line_item, '$.LineEx.any'))) AS confirm_entry
        WHERE JSON_VALUE(confirm_entry, '$.value.Name') = 'txnId'
          AND JSON_VALUE(confirm_entry, '$.value.Value') = JSON_VALUE(line_item, '$.LinkedTxn[0].TxnId')
      )
    LIMIT 1
  ) AS DocNumber

FROM {{ source('staging', 'payments') }}





-- SELECT 
--   DISTINCT
--   Id,
--   --CustomerRef,
--   JSON_VALUE(CustomerRef, '$.value') AS CustomerRefValue,
--   JSON_VALUE(CustomerRef, '$.name') AS CustomerRefName,
--   --DepositToAccountRef,
--   JSON_VALUE(DepositToAccountRef, '$.value') AS DepositToAccountRefValue,
--   TxnDate,
--   --CurrencyRef,
--   JSON_VALUE(CurrencyRef, '$.value') AS CurrencyRefValue,
--   JSON_VALUE(CurrencyRef, '$.name') AS CurrencyRefName,
--   ExchangeRate,
--   TotalAmt,
--   UnappliedAmt,
--   ProcessPayment,
--   json_extract_scalar(PaymentMethodRef, '$.value') PaymentMethodRef,
--   PaymentRefNum,
--   PrivateNote,
--   --MetaData,
--   JSON_VALUE(MetaData, '$.CreateTime') AS CreateTime,
--   JSON_VALUE(MetaData, '$.LastUpdatedTime') AS LastUpdatedTime,
--   -- Line,
--   -- Extract top-level fields
--   JSON_VALUE(Line, '$[0].Amount') AS Line_Amount,

--   -- Extract Linked Transaction details
--   JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnId') AS Line_TxnId,
--   JSON_VALUE(Line, '$[0].LinkedTxn[0].TxnType') AS Line_TxnType,

--   -- Extract values from LineEx.any array
--   JSON_VALUE(Line, '$[0].LineEx.any[0].value.Name') AS Line_TxnId_Name,
--   JSON_VALUE(Line, '$[0].LineEx.any[0].value.Value') AS Line_TxnId_Value,

-- (
--   SELECT JSON_VALUE(entry, '$.value.Value')
--   FROM UNNEST(
--     ARRAY(
--       SELECT AS STRUCT
--         JSON_EXTRACT_ARRAY(JSON_EXTRACT(line_item, '$.LineEx.any')) AS any_array,
--         OFFSET AS line_index
--       FROM UNNEST(JSON_EXTRACT_ARRAY(Line)) AS line_item WITH OFFSET
--     )
--   ) AS line_struct,
--   UNNEST(line_struct.any_array) AS entry WITH OFFSET AS inner_offset
--   WHERE JSON_VALUE(entry, '$.value.Name') = 'txnReferenceNumber'
--   ORDER BY line_struct.line_index DESC, inner_offset DESC
--   LIMIT 1
-- ) AS DocNumber
  
-- FROM {{ source('staging', 'payments') }}








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