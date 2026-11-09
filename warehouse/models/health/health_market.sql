-- latest result per rrp-market source plus 24h success rate. stale = no successful pull within the source's window:
-- 75 min for the 30-minute sources, 90 min for hourly sources (mempool included), 26 hours for the daily funding pull, 4 days for the
-- once-a-weekday equities pass and its symbol directory. One-off backfills (*_history) are left out. stale is TRUE when the
-- window holds no successful pull at all; last_error is the latest run's error only; fails_24h counts failed runs (2026-10-19).
WITH r AS (SELECT * FROM {{ source('ops', 'market_runs') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY) AND source NOT LIKE '%_history'),
w AS (SELECT * FROM r WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) OR source IN ('equities', 'universe', 'short_volume', 'short_interest', 'treasury_dts', 'eia', 'eia_weekly', 'degree_days', 'etf_shares'))
SELECT source, MAX(ts) AS last_run, MAX(IF(ok, ts, NULL)) AS last_ok,
       ROUND(AVG(IF(ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), IF(ok, 1, 0), NULL)), 3) AS ok_rate_24h,
       COUNTIF(ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS runs_24h,
       IF(ARRAY_AGG(ok ORDER BY ts DESC LIMIT 1)[OFFSET(0)], NULL, ARRAY_AGG(error IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)]) AS last_error,
       IFNULL(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(IF(ok, ts, NULL)), MINUTE) >
         CASE WHEN source IN ('equities', 'universe', 'short_volume', 'short_interest', 'treasury_dts', 'eia', 'eia_weekly', 'degree_days', 'etf_shares') THEN 4 * 1440
              WHEN source = 'funding' THEN 1560
              WHEN source IN ('crypto_market', 'farside_etf', 'defillama_stablecoins', 'fear_greed', 'macro_fred', 'macro_treasury', 'macro_cboe', 'mempool') THEN 90
              ELSE 75 END, TRUE) AS stale,
       COUNTIF(NOT ok AND ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS fails_24h
FROM w WHERE source NOT IN ('macro_lbma', 'cef_nav', 'macro_cboe') GROUP BY source
