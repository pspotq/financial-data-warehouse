-- Form 25 / 25-NSE (an exchange removing a security from listing) and Form 15 / 15F (deregistration) filings by company,
-- newest first. kind: delisting or deregistration. A Form 25 can cover notes or preferred shares rather than the common
-- stock; open url when it matters. ticker = the issuer's most recent Form 4 ticker, when it has one.
-- A Form 25-NSE is indexed under both the exchange that files it and the company; the exchange's rows (NYSE, Nasdaq,
-- NYSE Arca / American, Cboe BZX and older venues, by CIK) are left out (2026-10-26), so each row is a company.
WITH d AS (SELECT * FROM {{ source('sec', 'delistings') }}
           WHERE cik NOT IN ('876661', '1354457', '1143362', '1143313', '1417835', '1296945', '876882', '876798')   -- NYSE, Nasdaq,
             -- NYSE Arca, NYSE American (ex-AMEX), Cboe BZX (ex-BATS), Boston SE, NYSE Chicago / Texas, Philadelphia SE
           QUALIFY ROW_NUMBER() OVER (PARTITION BY accession, cik ORDER BY form) = 1),
t AS (SELECT issuer_cik AS cik, ARRAY_AGG(ticker ORDER BY filed DESC LIMIT 1)[OFFSET(0)] AS ticker
      FROM {{ source('sec', 'form4') }} WHERE REGEXP_CONTAINS(ticker, r'^[A-Z]{1,5}([.-][A-Z])?$') GROUP BY issuer_cik)
SELECT d.cik, d.company, t.ticker, d.form, IF(STARTS_WITH(d.form, '25'), 'delisting', 'deregistration') AS kind, d.filed,
       d.accession, CONCAT('https://www.sec.gov/Archives/', d.path) AS url
FROM d LEFT JOIN t USING (cik)
