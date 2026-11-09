-- point-in-time ticker <-> company (SEC CIK) map: one row per company per ticker it traded under, with the dates the
-- ticker was seen on that company (Form 4 filings since 2006, which name the issuer's ticker, plus the SEC's daily
-- ticker list since Oct 2026). A ticker reused by a later company (1,000+ have been) gets one row per company:
-- valid_from / valid_to never overlap for a ticker, so to find who 'ABC' was on a date use
--   WHERE ticker = 'ABC' AND d BETWEEN valid_from AND IFNULL(valid_to, CURRENT_DATE()).
-- valid_from = the first sighting (an approximation of the listing date for names with few Form 4s); valid_to = the
-- earliest of: the day before the next company took the ticker, the day before this company's next ticker (FB -> META),
-- its delisting (Form 25 / 15), else the last sighting; NULL while the ticker is current. Form 4 tickers are
-- typed by filers: pairs seen on only one filing and not on the SEC list are left out. delisted_on = the first Form 25 / 15
-- the company filed from 30 days before the ticker was last seen on (sec.delistings).
WITH f4 AS (SELECT issuer_cik AS cik, UPPER(TRIM(ticker)) AS ticker, MIN(filed) AS first_seen, MAX(filed) AS last_seen,
                   COUNT(DISTINCT accession) AS n
            FROM {{ source('sec', 'form4') }} WHERE REGEXP_CONTAINS(UPPER(TRIM(ticker)), r'^[A-Z]{1,5}([.-][A-Z]{1,2})?$') GROUP BY 1, 2),
sn AS (SELECT cik, ticker, MIN(as_of) AS first_seen, MAX(as_of) AS last_seen, ANY_VALUE(name) AS name,
              MAX(as_of) = (SELECT MAX(as_of) FROM {{ source('sec', 'cik_tickers') }}) AS is_current
       FROM {{ source('sec', 'cik_tickers') }} WHERE ticker != '' GROUP BY 1, 2),
u AS (SELECT COALESCE(f.cik, s.cik) AS cik, COALESCE(f.ticker, s.ticker) AS ticker,
             LEAST(IFNULL(f.first_seen, DATE '9999-12-31'), IFNULL(s.first_seen, DATE '9999-12-31')) AS first_seen,
             GREATEST(IFNULL(f.last_seen, DATE '0001-01-01'), IFNULL(s.last_seen, DATE '0001-01-01')) AS last_seen,
             IFNULL(s.is_current, FALSE) AS is_current, s.name, IFNULL(f.n, 0) AS form4_filings,
             CONCAT(IF(f.cik IS NOT NULL, 'form4', ''), IF(f.cik IS NOT NULL AND s.cik IS NOT NULL, '+', ''),
                    IF(s.cik IS NOT NULL, 'sec_list', '')) AS sources
      FROM f4 f FULL OUTER JOIN sn s ON s.cik = f.cik AND s.ticker = f.ticker
      WHERE s.cik IS NOT NULL OR f.n >= 2),
e AS (SELECT *, LEAD(first_seen) OVER (PARTITION BY ticker ORDER BY first_seen, cik) AS next_first,   -- next company on the ticker
             LEAD(first_seen) OVER (PARTITION BY cik ORDER BY first_seen, ticker) AS next_own FROM u),  -- this company's next ticker
nm AS (SELECT issuer_cik AS cik, ARRAY_AGG(issuer ORDER BY filed DESC LIMIT 1)[OFFSET(0)] AS issuer FROM {{ source('sec', 'form4') }} GROUP BY 1),
dl AS (SELECT e.cik, e.ticker, MIN(d.filed) AS delisted_on FROM e JOIN {{ source('sec', 'delistings') }} d
         ON d.cik = e.cik AND d.filed >= DATE_SUB(e.last_seen, INTERVAL 30 DAY) GROUP BY 1, 2)
SELECT e.ticker, e.cik, COALESCE(e.name, nm.issuer) AS company, e.first_seen AS valid_from,
       CASE WHEN e.is_current AND e.next_first IS NULL THEN NULL
            ELSE LEAST(IFNULL(DATE_SUB(e.next_first, INTERVAL 1 DAY), DATE '9999-12-31'),
                       -- a ticker change (FB -> META) caps the old one; share classes seen side by side (GOOG / GOOGL) do not
                       IF(e.last_seen < e.next_own, DATE_SUB(e.next_own, INTERVAL 1 DAY), DATE '9999-12-31'),
                       COALESCE(dl.delisted_on, IF(e.next_first IS NULL AND NOT IFNULL(e.last_seen < e.next_own, FALSE), e.last_seen, NULL), DATE '9999-12-31'))
       END AS valid_to,
       e.is_current, e.last_seen, e.form4_filings, e.sources, dl.delisted_on
FROM e LEFT JOIN nm USING (cik) LEFT JOIN dl USING (cik, ticker)
