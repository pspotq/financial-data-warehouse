-- latest spot reading per source and symbol (Kraken, Coinbase), with its age
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY source, symbol ORDER BY ts DESC) rn
  FROM {{ source('markets', 'crypto_spot') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1
