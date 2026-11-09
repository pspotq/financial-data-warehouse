-- 30-day insider buying clusters: 3+ distinct insiders, $250K+ in clean open-market buys, price-checked against the
-- market close. net_seller_flag = insiders sold more than they bought in the same 30 days
WITH b AS (
  SELECT * FROM {{ ref('insider_buys_priced') }}
  WHERE tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY) AND NOT IFNULL(price_mismatch, FALSE)),
s AS (
  SELECT ticker, SUM(-value) AS sold_30d, COUNT(DISTINCT insider) AS sellers_30d
  FROM {{ ref('insider_trades') }}
  WHERE code = 'S' AND form = '4' AND NOT value_suspect AND tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY)
  GROUP BY ticker),
last_px AS (
  SELECT symbol, close AS last_close, date AS last_close_date FROM {{ source('markets', 'equity_daily') }}
  WHERE date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1)
SELECT b.ticker, ANY_VALUE(b.issuer) AS issuer,
  COUNT(DISTINCT b.insider) AS buyers,
  ROUND(SUM(b.value)) AS bought_usd,
  ROUND(SUM(b.value) / COUNT(DISTINCT b.insider)) AS avg_per_buyer_usd,
  ROUND(SAFE_DIVIDE(SUM(b.value), SUM(b.shares)), 2) AS vwap,
  MIN(b.tx_date) AS first_tx, MAX(b.tx_date) AS last_tx, MAX(b.filed) AS last_filed,
  STRING_AGG(DISTINCT b.role, ' | ') AS roles,
  COUNT(DISTINCT IF(REGEXP_CONTAINS(LOWER(IFNULL(b.role, '')), r'ceo|chief executive|cfo|chief financial|president|chair'), b.insider, NULL)) AS top_exec_buyers,
  IFNULL(s.sold_30d, 0) AS sold_usd_30d, IFNULL(s.sellers_30d, 0) AS sellers_30d,
  IFNULL(s.sold_30d, 0) > SUM(b.value) AS net_seller_flag,
  ANY_VALUE(p.last_close) AS last_close, ANY_VALUE(p.last_close_date) AS last_close_date,
  ROUND(100 * (SAFE_DIVIDE(ANY_VALUE(p.last_close), SAFE_DIVIDE(SUM(b.value), SUM(b.shares))) - 1), 1) AS pct_vs_insider_vwap,
  ANY_VALUE(p.last_close) IS NULL AS unpriced
FROM b LEFT JOIN s USING (ticker) LEFT JOIN last_px p ON p.symbol = b.ticker
GROUP BY b.ticker, s.sold_30d, s.sellers_30d
HAVING buyers >= 3 AND bought_usd >= 250000
