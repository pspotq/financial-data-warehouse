-- every Daily Treasury Statement deposit and withdrawal line (Oct 2005 on), $ millions: one row per day, account,
-- direction and category. Category names changed in Oct 2021 and Feb 2023 (e.g. 'Taxes - Corporate Income' from Feb 2023);
-- compare within one naming era. Pre-2023 withheld and corporate taxes are in rrp.treasury_tax_deposits.
SELECT record_date, account_type AS account, transaction_type AS direction, category, today_amt, mtd_amt, fytd_amt
FROM {{ source('gov', 'treasury_dts') }} WHERE table_id = 'flows' AND category IS NOT NULL
