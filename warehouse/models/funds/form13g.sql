-- every Schedule 13G / 13G/A (passive owner of more than 5% of a listed class), structured filings Dec 2024 on, loaded
-- from 2025, one row per filing. filer = the lead reporting person (highest pct when several file jointly; all names in
-- persons). rule: 13d-1(b) = institution (adviser, bank, broker, insurer: filer_type IA / HC / BD / BK / IC / FI),
-- 13d-1(c) = other passive investor over 5%, 13d-1(d) = held before the IPO / registration. pct = percent of the
-- class; shares = aggregate amount owned. cusip = 9-character CUSIP cleaned from what the filer typed (ISINs cut
-- down, check digit kept); cusip8 = the first 8 (issue) characters, the safe key to join rrp.form13f_* on.
-- prev_pct / pct_change = this filer's previous 13G on the same issuer; exited = an amendment at or under 5%
-- (the filer no longer has to report). ticker = the issuer's ticker on the filing date.
WITH g AS (
  SELECT *, CASE WHEN REGEXP_CONTAINS(c, r'^[A-Z]{2}[0-9A-Z]{9}[0-9]$') THEN SUBSTR(c, 3, 9)
                 WHEN LENGTH(c) >= 9 THEN SUBSTR(c, 1, 9) ELSE c END AS cusip9
  FROM (SELECT *, UPPER(REGEXP_REPLACE(IFNULL(cusip, ''), r'[^0-9A-Za-z]', '')) AS c FROM {{ source('sec', 'form13g') }})
  WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1),
t AS (SELECT cik, ticker, valid_from, valid_to, form4_filings FROM {{ ref('entity_tickers') }}),
j AS (
  SELECT g.*, t.ticker FROM g
  LEFT JOIN t ON t.cik = g.issuer_cik AND g.filed BETWEEN t.valid_from AND IFNULL(t.valid_to, DATE '9999-12-31')
  WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY g.accession ORDER BY t.form4_filings DESC) = 1)
SELECT accession, filed, form, form LIKE '%/A' AS is_amendment, amendment_no, issuer_cik, issuer_name, ticker,
       NULLIF(cusip9, '') AS cusip, NULLIF(SUBSTR(cusip9, 1, 8), '') AS cusip8, cusip AS cusip_as_filed, security_class,
       event_date, filer, filer_type, rule, pct, shares, sole_vote, shared_vote, sole_dispositive, shared_dispositive,
       persons, LAG(pct) OVER w AS prev_pct, ROUND(pct - LAG(pct) OVER w, 2) AS pct_change,
       form LIKE '%/A' AND pct <= 5 AS exited,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', issuer_cik, '/', accession, '/') AS url
FROM j WINDOW w AS (PARTITION BY issuer_cik, filer ORDER BY filed, accession)
