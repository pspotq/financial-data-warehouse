-- every coin the momentum scan's number filters flag each morning, with its price every hour for the next 7 days.
-- Snapshot = the last CoinPaprika snapshot between 4:45 and 6:45 AM ET (what the 6:45 AM scan reads). f1 mcap $20-200M,
-- f2 vol/mcap > 1.5, f4 70%+ below an ATH set over a year ago, f5 data older than 6 months. status = cleared (all four) or
-- near_miss (three, with mcap $10-400M and vol/mcap > 0.75). hours_after 0 = the scan snapshot itself
WITH snaps AS (SELECT DISTINCT ts FROM {{ source('markets', 'crypto_market') }}),
scan AS (
  SELECT DATE(ts, 'America/New_York') AS scan_date, MAX(ts) AS scan_ts FROM snaps
  WHERE TIME(ts, 'America/New_York') BETWEEN '04:45:00' AND '06:45:00' GROUP BY 1),
screened AS (
  SELECT s.scan_date, s.scan_ts, m.id, m.symbol, m.name, m.rank, m.price AS scan_price, m.market_cap, m.volume_24h,
    ROUND(SAFE_DIVIDE(m.volume_24h, m.market_cap), 2) AS vol_to_mcap, m.pct_24h, m.pct_7d, m.pct_from_ath, m.ath_date, m.first_data_at,
    IFNULL(m.market_cap BETWEEN 20e6 AND 200e6, FALSE) AS f1_mcap,
    IFNULL(SAFE_DIVIDE(m.volume_24h, m.market_cap) > 1.5, FALSE) AS f2_volume,
    IFNULL(m.pct_from_ath <= -70 AND m.ath_date < DATE_SUB(s.scan_date, INTERVAL 1 YEAR), FALSE) AS f4_drawdown,
    IFNULL(m.first_data_at < DATE_SUB(s.scan_date, INTERVAL 6 MONTH), FALSE) AS f5_age
  FROM scan s JOIN {{ source('markets', 'crypto_market') }} m ON m.ts = s.scan_ts
  WHERE m.price > 0),
flagged AS (
  SELECT *, CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) AS passed
  FROM screened
  WHERE CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) >= 3
    AND (f1_mcap OR market_cap BETWEEN 10e6 AND 400e6)
    AND (f2_volume OR SAFE_DIVIDE(volume_24h, market_cap) > 0.75))
SELECT f.scan_date, f.scan_ts, f.id, f.symbol, f.name, f.rank,
  IF(f.passed = 4, 'cleared', 'near_miss') AS status,
  CASE WHEN f.passed = 4 THEN NULL WHEN NOT f.f1_mcap THEN 'f1_mcap' WHEN NOT f.f2_volume THEN 'f2_volume'
       WHEN NOT f.f4_drawdown THEN 'f4_drawdown' ELSE 'f5_age' END AS failed_filter,
  f.scan_price, f.market_cap, f.volume_24h, f.vol_to_mcap, f.pct_24h, f.pct_7d, f.pct_from_ath, f.ath_date, f.first_data_at,
  CAST(ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600) AS INT64) AS hours_after,
  h.ts, h.price,
  ROUND(100 * (h.price / f.scan_price - 1), 2) AS ret_pct
FROM flagged f
JOIN {{ source('markets', 'crypto_market') }} h
  ON h.id = f.id AND h.ts >= f.scan_ts AND h.ts < TIMESTAMP_ADD(f.scan_ts, INTERVAL 10110 MINUTE)
WHERE h.price > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.scan_date, f.id, CAST(ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600) AS INT64)
                           ORDER BY ABS(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) - 3600 * ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600))) = 1
