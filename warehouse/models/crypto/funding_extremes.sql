-- perpetual funding on the newest completed UTC day per coin (Kraken), measured against its own history:
-- highest_since / lowest_since = the last day funding was at least as high / as low (NULL = a record since history_from);
-- days_since_high / days_since_low; cap_rank = the coin's CoinPaprika market-cap rank (newest snapshot) for filtering.
WITH d AS (SELECT symbol, date, annualized_pct, MIN(date) OVER (PARTITION BY symbol) AS history_from
           FROM {{ ref('funding_daily') }} WHERE date < CURRENT_DATE('UTC')),
l AS (SELECT * FROM d WHERE date = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)),
x AS (SELECT l.symbol, MAX(IF(d.annualized_pct >= l.annualized_pct, d.date, NULL)) AS highest_since,
             MAX(IF(d.annualized_pct <= l.annualized_pct, d.date, NULL)) AS lowest_since,
             ROUND(AVG(IF(d.date >= DATE_SUB(l.date, INTERVAL 30 DAY), d.annualized_pct, NULL)), 2) AS avg_30d_pct
      FROM l JOIN d ON d.symbol = l.symbol AND d.date < l.date GROUP BY l.symbol),
r AS (SELECT UPPER(symbol) AS symbol, MIN(rank) AS cap_rank FROM {{ ref('crypto_market') }}
      WHERE ts = (SELECT MAX(ts) FROM {{ ref('crypto_market') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) GROUP BY 1)
SELECT l.symbol, l.date, l.annualized_pct, x.avg_30d_pct, x.highest_since, x.lowest_since, l.history_from,
       DATE_DIFF(l.date, IFNULL(x.highest_since, l.history_from), DAY) AS days_since_high,
       DATE_DIFF(l.date, IFNULL(x.lowest_since, l.history_from), DAY) AS days_since_low, r.cap_rank
FROM l JOIN x USING (symbol) LEFT JOIN r ON r.symbol = UPPER(l.symbol)
