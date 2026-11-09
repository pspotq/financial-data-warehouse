-- total crypto market cap, volume and BTC dominance (CoinPaprika), latest reading
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY ts DESC) rn FROM {{ source('markets', 'crypto_global') }}
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1
