-- CUSIP -> company (SEC CIK) map: one row per 8-character CUSIP (cusip8, the issue without its check digit) with
-- the issuer's CIK, name and ticker. Use it to put a CIK / ticker on rrp.form13f_* rows: JOIN ON cusip8 = SUBSTR(cusip, 1, 8).
-- source '13g': from 13G filings, which name both the CUSIP and the issuer (the CIK most filings gave it; filings and
--   agreement = how many filings name it and the share that agree). Issuers with a 13G filer over 5% since 2025.
-- source 'figi' (b190, 2026-11-07): CUSIPs the 13G map can't place, looked up on OpenFIGI (sec.cusip_figi): an equity
--   with a US listing whose ticker is a current SEC ticker (rrp.cik_tickers) and whose name starts with the same word
--   as the SEC name (a reused ticker would fail that check); warrants, rights and SPAC units left out. filings = 0,
--   agreement = 1. 13G rows win where both exist.
WITH g AS (
  SELECT cusip8, ARRAY_AGG(STRUCT(issuer_cik, issuer_name, ticker, cusip) ORDER BY n DESC, last_filed DESC LIMIT 1)[OFFSET(0)].*,
         SUM(n) AS filings, MAX(n) / SUM(n) AS agreement, MIN(first_filed) AS first_filed, MAX(last_filed) AS last_filed
  FROM (SELECT cusip8, issuer_cik, ARRAY_AGG(issuer_name IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS issuer_name,
               ARRAY_AGG(ticker IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS ticker,
               ARRAY_AGG(cusip IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS cusip,
               COUNT(*) AS n, MIN(filed) AS first_filed, MAX(filed) AS last_filed
        FROM {{ ref('form13g') }} WHERE cusip8 IS NOT NULL AND LENGTH(cusip8) = 8 GROUP BY cusip8, issuer_cik)
  GROUP BY cusip8),
f AS (
  SELECT x.cusip8, t.cik AS issuer_cik, t.name AS issuer_name, t.ticker, x.cusip
  FROM (SELECT * FROM {{ source('sec', 'cusip_figi') }} QUALIFY ROW_NUMBER() OVER (PARTITION BY cusip8 ORDER BY looked_up_at DESC) = 1) x
  JOIN {{ ref('cik_tickers') }} t ON t.is_current AND t.ticker = REPLACE(REPLACE(x.ticker, '/', '-'), '.', '-')
  WHERE x.found AND x.market_sector = 'Equity'
    -- the stock itself only: warrants, rights and SPAC units have their own tickers under the same CIK and would add
    -- their holders to the company's (a 5-letter ticker ending in U is a SPAC unit; other units are LP common units)
    AND x.security_type NOT IN ('Equity WRT', 'Right')
    AND NOT (x.security_type = 'Unit' AND LENGTH(x.ticker) = 5 AND ENDS_WITH(x.ticker, 'U'))
    AND x.exch_code IN ('US', 'UN', 'UQ', 'UW', 'UA', 'UR', 'UP', 'UF', 'UV', 'PQ')
    AND REGEXP_EXTRACT(UPPER(x.name), r'[A-Z0-9]+') = REGEXP_EXTRACT(UPPER(t.name), r'[A-Z0-9]+')
  QUALIFY COUNT(*) OVER (PARTITION BY x.cusip8) = 1)
SELECT g.*, '13g' AS source FROM g
UNION ALL
SELECT f.cusip8, f.issuer_cik, f.issuer_name, f.ticker, f.cusip, 0, 1.0, CAST(NULL AS DATE), CAST(NULL AS DATE), 'figi'
FROM f WHERE f.cusip8 NOT IN (SELECT cusip8 FROM g)
