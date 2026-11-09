-- one row per asset per trading day: Farside's total, the sum of the funds (they should match), funds reporting,
-- and the run of consecutive inflow (+) or outflow (-) days ending that day
WITH d AS (
  SELECT asset, date, MAX(IF(fund = 'TOTAL', flow_usd_m, NULL)) AS total_usd_m,
         ROUND(SUM(IF(fund != 'TOTAL', flow_usd_m, 0)), 1) AS funds_sum_usd_m, COUNTIF(fund != 'TOTAL' AND flow_usd_m != 0) AS funds_with_flow
  FROM {{ source('markets', 'etf_flows') }} GROUP BY asset, date),
s AS (SELECT *, SIGN(total_usd_m) AS sg,
        COUNTIF(SIGN(total_usd_m) != prev_sg OR prev_sg IS NULL) OVER (PARTITION BY asset ORDER BY date) AS grp
      FROM (SELECT *, LAG(SIGN(total_usd_m)) OVER (PARTITION BY asset ORDER BY date) AS prev_sg FROM d))
SELECT asset, date, total_usd_m, funds_sum_usd_m, funds_with_flow,
       CAST(sg * ROW_NUMBER() OVER (PARTITION BY asset, grp ORDER BY date) AS INT64) AS streak_days
FROM s
