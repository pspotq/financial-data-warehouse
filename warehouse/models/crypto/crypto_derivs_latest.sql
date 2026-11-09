-- BTC and ETH perpetuals (Deribit): latest mark, funding (8h and annualized) and open interest
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROUND(funding_8h * 3 * 365 * 100, 2) AS funding_annualized_pct,
         ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY ts DESC) rn
  FROM {{ source('markets', 'crypto_derivs') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1
