-- Form 4 open-market buys (P) and sales (S), one row per distinct transaction line, every EDGAR day since Jan 2006.
-- source 'sec_dataset' (2006-2020, SEC quarterly data sets: shares and prices rounded to 2 decimals); 'edgar_daily_index'
-- (2021 on, parsed from each filing: exact).
-- A jointly filed Form 4 lists the same trade once per filer: collapsed to one (copies = how many).
-- An amended Form 4 (4/A) line identical to a line of an original Form 4 (same ticker, insider, transaction date, code,
-- shares and price) is left out; a 4/A line that changes anything is kept as a correction.
-- value_suspect: the filer's own numbers are implausible (a line over $2B, price equal to shares, a price over $1M).
-- Kept as filed; leave it out of any sum, record or "largest" comparison. value is negative for sales.
SELECT * EXCEPT(rn, originals),
  IFNULL(ABS(value) > 2e9 OR (price = shares AND shares >= 100000) OR price > 1e6, FALSE) AS value_suspect
FROM (
  SELECT filed, tx_date, ticker, issuer, insider, role, code, shares, price, value, owned_after, direct_indirect,
    plan_10b5_1, private_placement, non_usd_suspect, accession, footnotes, kind, source, form, issuer_cik, security,
    COUNT(*) OVER w AS copies, ROW_NUMBER() OVER (w ORDER BY direct_indirect) AS rn,
    COUNTIF(form = '4') OVER (PARTITION BY ticker, insider, tx_date, code, CAST(shares AS STRING), CAST(price AS STRING)) AS originals
  FROM {{ source('sec', 'form4') }}
  WINDOW w AS (PARTITION BY accession, ticker, insider, tx_date, code, CAST(shares AS STRING), CAST(price AS STRING),
               CAST(owned_after AS STRING))
) WHERE rn = 1 AND NOT (form = '4/A' AND originals > 0)
