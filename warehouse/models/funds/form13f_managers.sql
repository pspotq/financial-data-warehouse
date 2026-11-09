-- one row per 13F manager per quarter: name, filing dates, total value (US$) and number of holding lines.
-- value_usd = the sum of the manager's own holding lines (sec.form13f_totals), falling back to the cover total when a
-- filing has no lines; cover_value_usd = the cover totals as typed (a few are typos). value_change_pct = vs the
-- manager's previous reported quarter. Cheap: no holdings scan.
WITH m AS (
  SELECT cik, period, ANY_VALUE(manager) AS manager, MIN(filed) AS first_filed, MAX(filed) AS last_filed,
         SUM(COALESCE(holdings_value_usd, value_total_usd)) AS value_usd, SUM(value_total_usd) AS cover_value_usd,
         SUM(entry_total) AS holdings, LOGICAL_OR(report_type LIKE '%COMBINATION%') AS combination,
         COUNTIF(role = 'added') AS amendments_added
  FROM {{ ref('form13f_reports') }} GROUP BY cik, period)
SELECT *, ROUND(100 * SAFE_DIVIDE(value_usd - LAG(value_usd) OVER w, LAG(value_usd) OVER w), 2) AS value_change_pct
FROM m WINDOW w AS (PARTITION BY cik ORDER BY period)
