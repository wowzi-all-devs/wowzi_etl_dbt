SELECT 
  Id,
  DocNumber,
  --CustomerRef,
  JSON_VALUE(CustomerRef, '$.value') AS CustomerRefValue,
  JSON_VALUE(CustomerRef, '$.name') AS CustomerRefName,
  BillAddr,
  ShipAddr,
  --BillEmail,
  JSON_VALUE(BillEmail, '$.Address') AS BillEmail,
  --SalesTermRef,
  JSON_VALUE(SalesTermRef, '$.value') AS SalesTermRef,
  TxnDate,
  DueDate,
  --MetaData,
  JSON_VALUE(MetaData, '$.CreateTime') AS CreateTime,
  JSON_VALUE(MetaData, '$.LastUpdatedTime') AS LastUpdatedTime,
  --CurrencyRef,
  JSON_VALUE(CurrencyRef, '$.value') AS CurrencyRefValue,
  JSON_VALUE(CurrencyRef, '$.name') AS CurrencyRefName,
  ExchangeRate,
  TotalAmt,
  HomeTotalAmt,
  Balance,
  LinkedTxn,
  Line,
  -- Extract from the first object in the array
  JSON_VALUE(Line, '$[0].Id') AS Line_Id,
  JSON_VALUE(Line, '$[0].LineNum') AS Line_LineNum,
  JSON_VALUE(Line, '$[0].Description') AS Line_Description,
  JSON_VALUE(Line, '$[0].Amount') AS Line_Amount,
  JSON_VALUE(Line, '$[0].DetailType') AS Line_DetailType,
  
  -- Extract SalesItemLineDetail fields
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.ServiceDate') AS Line_ServiceDate,
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.ItemRef.value') AS Line_ItemRefValue,
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.ItemRef.name') AS Line_ItemRefName,
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.UnitPrice') AS Line_UnitPrice,
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.Qty') AS Line_Qty,
  JSON_VALUE(Line, '$[0].SalesItemLineDetail.TaxCodeRef.value') AS Line_TaxCodeRefValue,
  
  -- Extract from the second object in the array (SubTotalLineDetail)
  JSON_VALUE(Line, '$[1].Amount') AS Line_SubTotalAmount,
  JSON_VALUE(Line, '$[1].DetailType') AS Line_SubTotalDetailType,
  TxnTaxDetail,
  GlobalTaxCalculation,
  CustomerMemo,
  EInvoiceStatus,
  DeliveryInfo,
  DepartmentRef
FROM {{ source('staging', 'invoices') }}











/**
SELECT  
  id,
  --Line,
  JSON_EXTRACT_SCALAR(Line, '$[0].Amount') AS line_line_amount,
  JSON_EXTRACT_SCALAR(Line, '$[0].Description') AS line_description,
  JSON_EXTRACT_SCALAR(Line, '$[0].DetailType') AS line_detailtype,
  JSON_EXTRACT_SCALAR(Line, '$[0].Id') AS line_id,
  JSON_EXTRACT_SCALAR(Line, '$[0].LineNum') AS line_linenum,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail.ClassRef'), '$.name') AS line_classrefname,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail.ClassRef'), '$.value') AS line_classrefvalue,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail.ItemRef'), '$.name') AS line_itemRefname,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail.ItemRef'), '$.value') AS line_itemrefvalue,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail'), '$.Qty') AS line_qty,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail'), '$.ServiceDate') AS line_servicedate,
  CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(Line, '$[0].SalesItemLineDetail'), '$.UnitPrice') AS FLOAT64) AS line_unitprice,
  CAST(JSON_EXTRACT_SCALAR(Line, '$[0].Amount') AS FLOAT64) AS line_amount,
  JSON_EXTRACT_SCALAR(Line, '$[0].DetailType') AS line_detail_type,
  domain,
  sparse,
  Balance balance,
  DueDate due_date,
  TxnDate txn_date,
  --BillAddr,
  JSON_EXTRACT_SCALAR(BillAddr, '$.Id') AS bill_addr_id,
  JSON_EXTRACT_SCALAR(BillAddr, '$.Line1') AS bill_addr_line1,
  JSON_EXTRACT_SCALAR(BillAddr, '$.Line2') AS bill_addr_line2,
  JSON_EXTRACT_SCALAR(BillAddr, '$.Line3') AS bill_addr_line3,
  --MetaData,
  JSON_EXTRACT_SCALAR(MetaData, '$.CreateTime') AS create_time,
  JSON_EXTRACT_SCALAR(MetaData, '$.LastUpdatedTime') AS last_updatedtime,
  ShipAddr ship_addr,
  TotalAmt total_amt,
  BillEmail bill_email,
  DocNumber doc_number,
  --LinkedTxn,
  JSON_EXTRACT_SCALAR(LinkedTxn, '$[0].TxnId') AS linked_txn_txnId,
  JSON_EXTRACT_SCALAR(LinkedTxn, '$[0].TxnType') AS linked_txntype,
  SyncToken synctoken,
  --CurrencyRef,
  JSON_EXTRACT_SCALAR(CurrencyRef, '$.name') AS currency_name,
  CustomField,
  EmailStatus,
  PrintStatus,
  PrivateNote,
  CustomerMemo,
  DeliveryInfo,
  ExchangeRate,
  HomeTotalAmt,
  SalesTermRef,
  TxnTaxDetail,
  AllowIPNPayment,
  AllowOnlineACHPayment,
  AllowOnlineCreditCardPayment,
  AllowOnlinePayment
FROM {{ source('staging', 'invoices') }}
**/