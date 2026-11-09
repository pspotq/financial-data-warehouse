-- clean open-market buys with the market close on (or just before) the trade date. price_mismatch = the Form 4
-- price is more than 1.5x away from the close, which usually means a non-USD price or a unit error
WITH j AS (
  SELECT b.*, e.close AS mkt_close, e.date AS mkt_date
  FROM {{ ref('insider_buys') }} b
  LEFT JOIN {{ source('markets', 'equity_daily') }} e
    ON e.symbol = b.ticker AND e.date <= b.tx_date AND e.date > DATE_SUB(b.tx_date, INTERVAL 7 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.accession, b.insider, b.tx_date, CAST(b.shares AS STRING), CAST(b.price AS STRING) ORDER BY e.date DESC) = 1)
SELECT *, ROUND(SAFE_DIVIDE(price, mkt_close), 3) AS px_ratio,
       mkt_close IS NOT NULL AND SAFE_DIVIDE(price, mkt_close) NOT BETWEEN 0.667 AND 1.5 AS price_mismatch
FROM j
