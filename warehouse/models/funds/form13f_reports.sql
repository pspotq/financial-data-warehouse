-- which 13F filings make up each manager's holdings for each quarter (period), amendments resolved: the base report is
-- the newest 13F-HR or RESTATEMENT amendment (13F-HR/A, amendment_type RESTATEMENT replaces the whole report); NEW
-- HOLDINGS amendments filed after it add lines. 13F-NT (notice: holdings reported by another manager) is left out.
-- role: base / added. report_type COMBINATION = the manager reports some holdings itself and some through others.
-- value_total_usd = the cover-page total as typed (pre-2023 units fixed 2026-10-26, sec.form13f_unit_fixes; a few
-- typos remain, e.g. Dimensional 2022-Q3 ~23x); holdings_value_usd = the sum of the filing's own holding lines
-- (sec.form13f_totals) - use it. Some filers (Norges Bank) file a one-line placeholder 13F-HR and the full table
-- later as a 13F-HR/A: until that arrives the quarter shows ~$0.
WITH f AS (SELECT f.*, t.holdings_value_usd, t.holdings_lines
           FROM {{ source('sec', 'form13f_filings') }} f LEFT JOIN {{ source('sec', 'form13f_totals') }} t USING (accession) WHERE form LIKE '13F-HR%'
           QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1),
b0 AS (SELECT * FROM f WHERE form = '13F-HR' OR amendment_type = 'RESTATEMENT' OR (form = '13F-HR/A' AND amendment_type IS NULL)
       QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period ORDER BY filed DESC, accession DESC) = 1),
-- a NEW HOLDINGS amendment that re-files the whole table (>= 90% of the base's lines; 1,398 of 4,722 such amendments,
-- e.g. FMR 2023-Q1) replaces the base instead of adding to it - added on top it double-counted ~$38T across quarters
full_refile AS (SELECT a.* FROM f a JOIN b0 USING (cik, period)
                WHERE a.amendment_type = 'NEW HOLDINGS' AND a.filed >= b0.filed AND b0.holdings_lines > 0
                  AND a.holdings_lines >= 0.9 * b0.holdings_lines),
base AS (SELECT * FROM (SELECT * FROM b0 UNION ALL SELECT * FROM full_refile)
         WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period ORDER BY filed DESC, accession DESC) = 1)
SELECT b.cik, b.period, b.manager, b.accession, b.filed, b.form, b.report_type, 'base' AS role, b.entry_total, b.value_total_usd,
       b.holdings_value_usd, b.holdings_lines
FROM base b
UNION ALL
SELECT a.cik, a.period, b.manager, a.accession, a.filed, a.form, a.report_type, 'added', a.entry_total, a.value_total_usd,
       a.holdings_value_usd, a.holdings_lines
FROM f a JOIN base b ON a.cik = b.cik AND a.period = b.period
WHERE a.amendment_type = 'NEW HOLDINGS' AND a.filed >= b.filed AND a.accession != b.accession
  AND NOT (b.holdings_lines > 0 AND a.holdings_lines >= 0.9 * b.holdings_lines)
