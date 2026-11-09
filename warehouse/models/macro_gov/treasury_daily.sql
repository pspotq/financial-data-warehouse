-- one row per business day from the Daily Treasury Statement (Oct 2005 on), $ millions. tga_close = the Treasury's
-- cash at the Fed (Federal Reserve Account before Oct 2021, Treasury General Account after; DTS moved the closing figure
-- into its 'today' column in Apr 2022, handled here). tga_change = vs the prior business day. debt_issues / debt_redemptions
-- = public debt cash issues and redemptions (Table IIIB); net_debt_issuance = issues - redemptions. tax_receipts = every
-- 'Taxes - ...' deposit line, which exists only from Feb 14 2023, when Treasury redesigned the statement (NULL before).
WITH c AS (
  SELECT record_date,
    COALESCE(MAX(IF(account_type = 'Treasury General Account (TGA) Closing Balance', open_today_bal, NULL)),
             MAX(IF(account_type = 'Treasury General Account (TGA)', close_today_bal, NULL)),
             MAX(IF(account_type = 'Federal Reserve Account', close_today_bal, NULL))) AS tga_close,
    MAX(IF(account_type = 'Tax and Loan Note Accounts (Table V)', close_today_bal, NULL)) AS tax_and_loan_close
  FROM {{ source('gov', 'treasury_dts') }} WHERE table_id = 'cash' GROUP BY record_date),
f AS (
  SELECT record_date,
    SUM(IF(transaction_type = 'Deposits' AND STARTS_WITH(category, 'Public Debt Cash Issues'), today_amt, 0)) AS debt_issues,
    SUM(IF(transaction_type = 'Withdrawals' AND STARTS_WITH(category, 'Public Debt Cash Redemp'), today_amt, 0)) AS debt_redemptions,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND STARTS_WITH(category, 'Taxes - '), today_amt, 0)), NULL) AS tax_receipts,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND category = 'Taxes - Withheld Individual/FICA', today_amt, 0)), NULL) AS withheld_taxes,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND category = 'Taxes - Corporate Income', today_amt, 0)), NULL) AS corporate_taxes,
    SUM(IF(transaction_type = 'Deposits' AND REGEXP_CONTAINS(category, r'Customs'), today_amt, 0)) AS customs_receipts
  FROM {{ source('gov', 'treasury_dts') }} WHERE table_id = 'flows' AND category IS NOT NULL GROUP BY record_date)
SELECT record_date, tga_close, tga_close - LAG(tga_close) OVER (ORDER BY record_date) AS tga_change, tax_and_loan_close,
       debt_issues, debt_redemptions, debt_issues - debt_redemptions AS net_debt_issuance,
       tax_receipts, withheld_taxes, corporate_taxes, customs_receipts
FROM c LEFT JOIN f USING (record_date)
