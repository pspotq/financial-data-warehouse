-- DTS Table IV federal tax deposits by type, Oct 2005 to Feb 13 2023 (the table was retired in the Feb 2023 redesign;
-- later tax receipts are the 'Taxes - ...' lines of rrp.treasury_flows / tax_receipts in rrp.treasury_daily). $ millions.
SELECT record_date, category AS tax_type, category_desc, today_amt, mtd_amt, fytd_amt
FROM {{ source('gov', 'treasury_dts') }} WHERE table_id = 'tax'
