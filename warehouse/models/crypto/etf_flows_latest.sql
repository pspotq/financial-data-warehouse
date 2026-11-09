-- spot crypto ETF flows, newest day per asset before today (New York): today's flows are never final until the next
-- morning. A day with a zero total and no fund reporting is skipped as not posted yet. streak_days: + inflow run, - outflow run. top_fund / top_fund_usd_m = the day's largest absolute
-- fund flow; ibit / fbtc for bitcoin. week_usd_m = Monday to this day; largest_since = the last day with a flow at least
-- this big in the same direction (NULL = largest on record). may_be_partial = read before 10 AM New York time on the
-- next weekday: Farside fills in late funds overnight and stores a missing fund as 0, so the day can still change.
WITH d AS (SELECT *, LAG(total_usd_m) OVER (PARTITION BY asset ORDER BY date) AS prev_day_usd_m,
                  ROUND(SUM(total_usd_m) OVER (PARTITION BY asset, DATE_TRUNC(date, WEEK(MONDAY)) ORDER BY date), 1) AS week_usd_m,
                  ROUND(SUM(total_usd_m) OVER (PARTITION BY asset ORDER BY UNIX_DATE(date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW), 1) AS last_30d_usd_m,
                  ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS age_rank
           FROM {{ ref('etf_flows_daily') }} WHERE NOT (IFNULL(total_usd_m, 0) = 0 AND funds_with_flow = 0)
             AND date < CURRENT_DATE('America/New_York')),
l AS (SELECT * FROM d WHERE age_rank = 1),
big AS (SELECT l.asset, MAX(d.date) AS largest_since FROM l JOIN d ON d.asset = l.asset AND d.date < l.date
          AND SIGN(d.total_usd_m) = SIGN(l.total_usd_m) AND ABS(d.total_usd_m) >= ABS(l.total_usd_m) GROUP BY l.asset),
fu AS (SELECT f.asset, ARRAY_AGG(STRUCT(f.fund, f.flow_usd_m) ORDER BY ABS(f.flow_usd_m) DESC LIMIT 1)[OFFSET(0)] AS top,
              MAX(IF(f.fund = 'IBIT', f.flow_usd_m, NULL)) AS ibit_usd_m, MAX(IF(f.fund = 'FBTC', f.flow_usd_m, NULL)) AS fbtc_usd_m
       FROM {{ ref('etf_flows') }} f JOIN l ON f.asset = l.asset AND f.date = l.date GROUP BY f.asset)
SELECT l.asset, l.date, l.total_usd_m, l.prev_day_usd_m, l.streak_days, l.week_usd_m, l.last_30d_usd_m, big.largest_since,
       fu.top.fund AS top_fund, fu.top.flow_usd_m AS top_fund_usd_m, fu.ibit_usd_m, fu.fbtc_usd_m,
       l.funds_with_flow,
       CURRENT_DATETIME('America/New_York') < DATETIME(DATE_ADD(l.date, INTERVAL IF(EXTRACT(DAYOFWEEK FROM l.date) = 6, 3, 1) DAY),
                                                      TIME '10:00:00') AS may_be_partial
FROM l LEFT JOIN big USING (asset) LEFT JOIN fu USING (asset)
