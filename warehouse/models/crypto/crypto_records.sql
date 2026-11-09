-- for each Kraken core coin, its newest completed UTC day measured against its own history (look-back facts):
-- day_pct = close vs previous close; biggest_move_since = last day with a move at least this large either way;
-- highest_close_since / lowest_close_since = last close at or above / at or below today's (NULL = a record since
-- history_from); above_ma50_days = consecutive closes on today's side of the 50-day average (+ above, - below);
-- ret_7d / ret_30d close to close.
WITH d AS (SELECT symbol, date, close, ma50, ROUND(100 * (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date) - 1), 2) AS day_pct,
                  LAG(close, 7) OVER (PARTITION BY symbol ORDER BY date) AS c7, LAG(close, 30) OVER (PARTITION BY symbol ORDER BY date) AS c30,
                  MIN(date) OVER (PARTITION BY symbol) AS history_from
           FROM {{ ref('crypto_core_daily') }}),
s AS (SELECT *, COUNTIF(side != prev_side OR prev_side IS NULL) OVER (PARTITION BY symbol ORDER BY date) AS grp
      FROM (SELECT *, SIGN(close - ma50) AS side, LAG(SIGN(close - ma50)) OVER (PARTITION BY symbol ORDER BY date) AS prev_side FROM d)),
r AS (SELECT *, CAST(side * ROW_NUMBER() OVER (PARTITION BY symbol, grp ORDER BY date) AS INT64) AS ma50_streak FROM s),
l AS (SELECT * FROM r QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1),
x AS (SELECT l.symbol,
        MAX(IF(ABS(d.day_pct) >= ABS(l.day_pct), d.date, NULL)) AS biggest_move_since,
        MAX(IF(d.close >= l.close, d.date, NULL)) AS highest_close_since,
        MAX(IF(d.close <= l.close, d.date, NULL)) AS lowest_close_since
      FROM l JOIN d ON d.symbol = l.symbol AND d.date < l.date GROUP BY l.symbol)
SELECT l.symbol, l.date, l.close, l.day_pct, x.biggest_move_since, x.highest_close_since, x.lowest_close_since, l.history_from,
       l.ma50, IF(l.ma50 IS NULL, NULL, l.ma50_streak) AS above_ma50_days,
       ROUND(100 * (l.close / l.c7 - 1), 2) AS ret_7d, ROUND(100 * (l.close / l.c30 - 1), 2) AS ret_30d
FROM l JOIN x USING (symbol)
