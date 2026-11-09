-- stablecoin supply now and its change over a day, a week and a month, per coin (DefiLlama, hourly), plus a TOTAL
-- row for every dollar-pegged coin (changes summed over the coins that have the earlier reading). frozen = the reading
-- equals the day-ago reading within $5 (a stuck source, not a flat day); rank 1 = largest coin. as_of_et = New York date.
WITH c AS (SELECT ts, id, symbol AS coin, name, peg_type, circulating AS supply_usd,
                  circulating - prev_day AS change_1d_usd, circulating - prev_week AS change_7d_usd,
                  circulating - prev_month AS change_30d_usd, prev_week
           FROM {{ ref('stablecoins_latest') }} WHERE peg_type = 'peggedUSD'),
u AS (SELECT * FROM c
      UNION ALL
      SELECT MAX(ts), 'total', 'TOTAL', 'all dollar stablecoins', 'peggedUSD', SUM(supply_usd), SUM(change_1d_usd),
             SUM(change_7d_usd), SUM(change_30d_usd), SUM(prev_week) FROM c)
SELECT ts, DATE(ts, 'America/New_York') AS as_of_et, id, coin, name, supply_usd, change_1d_usd, change_7d_usd, change_30d_usd,
       ROUND(100 * SAFE_DIVIDE(change_7d_usd, prev_week), 2) AS change_7d_pct,
       coin != 'TOTAL' AND ABS(change_1d_usd) < 5 AS frozen,
       IF(coin = 'TOTAL', 0, ROW_NUMBER() OVER (PARTITION BY coin = 'TOTAL' ORDER BY supply_usd DESC)) AS rank
FROM u
