WITH numbered_rows AS
(
SELECT 
  Account,
  Comments,
  row_number() over() row_num
FROM {{ source('staging', 'gsheets_balance_sheet_hygiene_feb_2025') }}
),

row_share_capital  AS 
(
SELECT 
    MIN(row_num) stop_row
FROM numbered_rows
    WHERE lower(Account) like '%share capital%'
)

SELECT 
    n.Account,
    n.Comments
FROM numbered_rows n
    WHERE n.row_num <= (SELECT stop_row FROM row_share_capital)