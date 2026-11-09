-- the 24h figure for every Kraken core coin: last trade vs the price 24 hours earlier, from the hourly bars (the bar
-- that held that moment: its open when the moment fell in the first half of the hour, else its close). high/low over
-- the same 24 hours. coinbase_gap_pct = Coinbase vs Kraken last (BTC, ETH); price_check_pct = CoinPaprika's
-- aggregated price vs Kraken last. ok = fresh (age_min <= 30), a full set of bars, and both checks within 1.5%.
WITH s AS (SELECT symbol, ts, last, age_min FROM {{ ref('crypto_spot_latest') }} WHERE source = 'kraken'),
h AS (SELECT symbol, bar_start, open, high, low, close FROM {{ ref('crypto_core_hourly') }}
      WHERE bar_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)),
w AS (SELECT s.symbol, s.ts, s.last, s.age_min,
        ANY_VALUE(IF(h.bar_start = TIMESTAMP_TRUNC(TIMESTAMP_SUB(s.ts, INTERVAL 24 HOUR), HOUR),
                     IF(EXTRACT(MINUTE FROM s.ts) < 30, h.open, h.close), NULL)) AS price_24h_ago,
        MAX(h.high) AS hi, MIN(h.low) AS lo, COUNT(*) AS bars
      FROM s JOIN h ON h.symbol = s.symbol AND h.bar_start >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(s.ts, INTERVAL 24 HOUR), HOUR)
      GROUP BY 1, 2, 3, 4),
cp AS (SELECT symbol, price FROM {{ ref('crypto_market') }}
       WHERE ts = (SELECT MAX(ts) FROM {{ ref('crypto_market') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
       QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY rank) = 1),
cb AS (SELECT symbol, last FROM {{ ref('crypto_spot_latest') }} WHERE source = 'coinbase_exchange')
SELECT w.symbol, w.ts, w.age_min, w.last, w.price_24h_ago,
       ROUND(100 * (w.last / w.price_24h_ago - 1), 2) AS pct_24h,
       ROUND(w.last - w.price_24h_ago, 6) AS change_24h,
       GREATEST(w.hi, w.last) AS high_24h, LEAST(w.lo, w.last) AS low_24h, w.bars,
       ROUND(100 * (cb.last / w.last - 1), 3) AS coinbase_gap_pct,
       ROUND(100 * (cp.price / w.last - 1), 3) AS price_check_pct,
       w.age_min <= 30 AND w.bars >= 24 AND w.price_24h_ago IS NOT NULL
         AND IFNULL(ABS(cb.last / w.last - 1) <= 0.015, TRUE) AND IFNULL(ABS(cp.price / w.last - 1) <= 0.015, TRUE) AS ok
FROM w LEFT JOIN cp USING (symbol) LEFT JOIN cb USING (symbol)
