-- every ticker the daily equities pass prices: the watchlist plus every listed US stock/ETF in the latest Nasdaq
-- Trader directory, without test issues, preferreds ($) and warrants/rights/units. Read by the rrp-market job.
WITH u AS (
  SELECT symbol, exchange, is_etf FROM {{ source('markets', 'equity_universe') }}
  WHERE as_of = (SELECT MAX(as_of) FROM {{ source('markets', 'equity_universe') }})
    AND NOT test_issue AND NOT REGEXP_CONTAINS(symbol, r'[$^=]')
    AND NOT REGEXP_CONTAINS(LOWER(name), r'\b(warrants?|rights?|units?)\b'))
SELECT ticker, MIN(priority) AS priority, STRING_AGG(DISTINCT reasons, ', ') AS reasons FROM (
  SELECT ticker, priority, reasons FROM {{ ref('equity_watchlist') }}
  UNION ALL SELECT symbol, 5, CONCAT('listed:', exchange, IF(is_etf, ' ETF', '')) FROM u)
GROUP BY ticker
