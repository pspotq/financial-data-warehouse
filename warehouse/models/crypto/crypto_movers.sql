-- the movers list: newest CoinPaprika snapshot, top 200 by market cap, $10M+ 24h volume, pegged coins left out,
-- ordered by the size of the 24h move. pct_24h = price now vs the snapshot 24 hours earlier (+/- 90 min), the same
-- definition as rrp.crypto_24h (CoinPaprika's own pct_24h runs about an hour behind; it is kept as paprika_pct_24h).
-- move_rank 1 = biggest move either way. age_min = snapshot age; read nothing when it is over 180.
WITH m AS (SELECT * FROM {{ ref('crypto_market') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)),
now_ AS (SELECT * FROM m WHERE ts = (SELECT MAX(ts) FROM m)),
then_ts AS (SELECT ARRAY_AGG(ts ORDER BY ABS(TIMESTAMP_DIFF(ts, TIMESTAMP_SUB((SELECT MAX(ts) FROM m), INTERVAL 24 HOUR), MINUTE)) LIMIT 1)[OFFSET(0)] AS t
            FROM (SELECT DISTINCT ts FROM m)
            WHERE ABS(TIMESTAMP_DIFF(ts, TIMESTAMP_SUB((SELECT MAX(ts) FROM m), INTERVAL 24 HOUR), MINUTE)) <= 90),
then_ AS (SELECT id, price FROM m WHERE ts = (SELECT t FROM then_ts))
SELECT n.ts, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), n.ts, MINUTE) AS age_min, (SELECT t FROM then_ts) AS ts_24h_ago,
       n.id, n.symbol, n.name, n.rank, n.price, t.price AS price_24h_ago,
       ROUND(100 * COALESCE(n.price / t.price - 1, n.pct_24h / 100), 2) AS pct_24h, n.pct_24h AS paprika_pct_24h,
       n.pct_7d, n.volume_24h, n.market_cap,
       ROW_NUMBER() OVER (ORDER BY ABS(COALESCE(n.price / t.price - 1, n.pct_24h / 100)) DESC) AS move_rank
FROM now_ n LEFT JOIN then_ t USING (id)
WHERE n.rank <= 200 AND n.volume_24h >= 10e6 AND UPPER(n.symbol) NOT IN (SELECT symbol FROM {{ ref('ref_pegged') }})
