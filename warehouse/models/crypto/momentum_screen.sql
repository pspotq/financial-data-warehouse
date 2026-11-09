-- the daily momentum scan's number filters on the newest CoinPaprika snapshot (all 2,000 coins): f1 market cap
-- $20M-$200M, f2 24h volume over 1.5x market cap, f4 70%+ below an all-time high set more than a year ago, f5 listed
-- 6+ months. passed = how many of the four; 4 = clears the numbers, 3 = near miss (shown only when the failed filter
-- was close: cap $10M-$400M and volume over 0.75x). The catalyst (filter 3) is judged by the task, not here.
WITH snap AS (SELECT * FROM {{ ref('crypto_market') }} WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
              QUALIFY ts = MAX(ts) OVER ()),
s AS (SELECT ts AS snapshot_ts, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min, id, symbol, name, rank, price,
        market_cap, volume_24h, ROUND(SAFE_DIVIDE(volume_24h, market_cap), 2) AS vol_to_mcap, pct_24h, pct_7d, ath_price,
        ath_date, pct_from_ath, first_data_at,
        IFNULL(market_cap BETWEEN 20e6 AND 200e6, FALSE) AS f1_mcap,
        IFNULL(SAFE_DIVIDE(volume_24h, market_cap) > 1.5, FALSE) AS f2_volume,
        IFNULL(pct_from_ath <= -70 AND ath_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), FALSE) AS f4_drawdown,
        IFNULL(first_data_at < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), FALSE) AS f5_age
      FROM snap WHERE UPPER(symbol) NOT IN (SELECT symbol FROM {{ ref('ref_pegged') }}))
SELECT *, CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) AS passed
FROM s
WHERE CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) >= 3
  AND (f1_mcap OR market_cap BETWEEN 10e6 AND 400e6)
  AND (f2_volume OR SAFE_DIVIDE(volume_24h, market_cap) > 0.75)
