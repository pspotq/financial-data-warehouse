-- latest result per rrp-feeds source; stale = no successful run in 3 hours (dod, tickers, delist and the
-- tender / stake / redemption term extractors and funds (Form D + 13F, added 2026-10-23), and since 2026-10-26 form4
-- (overnight daily index), cot (Fridays + a daily catch-up), war_rss (twice a day) and cusip_figi (08:05 UTC, added
-- 2026-11-07) - daily: 30 hours;
-- xbrl - Mondays: 8 days), and TRUE when there is no successful run in the window at all. last_error is the latest run's
-- error only (NULL once a feed recovers); fails_48h counts failed runs in the window (2026-10-19).
WITH r AS (SELECT * FROM {{ source('ops', 'feeds_runs') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 9 DAY) AND source NOT LIKE 'kalshi%'
           AND source NOT LIKE '%_history'   -- one-off backfills (efts_history) are not health sources
           AND (ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) OR source = 'xbrl'))
SELECT source, MAX(ts) AS last_run, MAX(IF(ok, ts, NULL)) AS last_ok, ROUND(AVG(IF(ok, 1, 0)), 3) AS ok_rate_48h,
       ARRAY_AGG(note IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_note,
       IF(ARRAY_AGG(ok ORDER BY ts DESC LIMIT 1)[OFFSET(0)], NULL, ARRAY_AGG(error IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)]) AS last_error,
       IFNULL(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(IF(ok, ts, NULL)), MINUTE) >
         CASE WHEN source IN ('dod', 'tickers', 'delist', 'tender_terms', 'stake_terms', 'redemption_terms', 'funds',
                              'form4', 'cot', 'war_rss', 'edgar_index', 'cusip_figi') THEN 1800
              WHEN source = 'xbrl' THEN 11520 ELSE 180 END, TRUE) AS stale,
       COUNTIF(NOT ok) AS fails_48h
FROM r GROUP BY source
