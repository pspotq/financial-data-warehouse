-- every EDGAR filing, one row per filing (b192 / b193, added 2026-10-29): EDGAR's own form index (daily and the quarterly
-- full index back to 1993) with each filing's parties, plus the full-text-search streams that matched it as tags.
-- parties = every CIK the index lists the filing under (filer, subject, reporting owners). streams = the rrp-sec
-- full-text streams that hit it ([] when none; a filing can match several), items = the 8-K item numbers when a stream
-- saw it. Filter on date_filed: the source tables are partitioned on it, so a date range keeps reads small.
WITH idx AS (
  SELECT acc18, date_filed, ANY_VALUE(accession) AS accession, ANY_VALUE(form) AS form,
         ARRAY_AGG(STRUCT(cik, company) ORDER BY cik) AS parties
  FROM {{ source('sec', 'filing_index') }} GROUP BY acc18, date_filed),
tags AS (
  SELECT REPLACE(accession, '-', '') AS acc18, file_date, ARRAY_AGG(DISTINCT query_id ORDER BY query_id) AS streams,
         ANY_VALUE(items) AS items
  FROM {{ source('sec', 'filing_hits') }} GROUP BY 1, 2)
SELECT i.acc18, i.accession, i.form, i.date_filed, i.parties, ARRAY_LENGTH(i.parties) AS n_parties,
       IFNULL(t.streams, []) AS streams, t.items,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', i.parties[OFFSET(0)].cik, '/', i.acc18, '/') AS index_url
FROM idx i LEFT JOIN tags t ON t.acc18 = i.acc18 AND t.file_date = i.date_filed
