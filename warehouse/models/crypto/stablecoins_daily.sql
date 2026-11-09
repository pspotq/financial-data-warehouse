-- stablecoin supply, one row per coin per day (the last reading of each UTC day). Every coin DefiLlama tracks from
-- the live feed (Sep 28 2026 on); USDT, USDC and DAI back to 2017 from the history table (source 'history').
WITH live AS (
  SELECT DATE(ts) AS date, id, symbol AS coin, name, peg_type, circulating AS circulating_usd, prev_day, prev_week, prev_month,
         'defillama' AS source, ts
  FROM {{ source('markets', 'stablecoins') }} WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id, DATE(ts) ORDER BY ts DESC) = 1)
SELECT * FROM live
UNION ALL
SELECT h.date, CASE h.coin WHEN 'USDT' THEN '1' WHEN 'USDC' THEN '2' WHEN 'DAI' THEN '5' END, h.coin, CAST(NULL AS STRING),
       'peggedUSD', h.circulating_usd, NULL, NULL, NULL, 'history', CAST(NULL AS TIMESTAMP)
FROM {{ source('markets', 'history_stablecoins') }} h
WHERE h.date < (SELECT MIN(date) FROM live WHERE id IN ('1', '2', '5'))
