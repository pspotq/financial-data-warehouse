-- US Treasury 2Y / 10Y / 30Y: newest close, its change in basis points vs the previous market day and since the last
-- close of the prior year, and the last day the yield closed at or above (highest_since) / at or below (lowest_since)
-- today's level (NULL = a record in the data, which starts in 1962).
WITH y AS (SELECT date, tenor, value, LAG(value) OVER (PARTITION BY tenor ORDER BY date) AS prev,
                  LAST_VALUE(IF(EXTRACT(YEAR FROM date) < EXTRACT(YEAR FROM CURRENT_DATE()), value, NULL) IGNORE NULLS)
                    OVER (PARTITION BY tenor ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prior_year_close
           FROM {{ ref('yields_daily') }}, UNNEST([STRUCT('2Y' AS tenor, y2 AS value), ('10Y', y10), ('30Y', y30)])
           WHERE value IS NOT NULL),
l AS (SELECT * FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY tenor ORDER BY date DESC) = 1),
x AS (SELECT l.tenor, MAX(IF(y.value >= l.value, y.date, NULL)) AS highest_since, MAX(IF(y.value <= l.value, y.date, NULL)) AS lowest_since
      FROM l JOIN y ON y.tenor = l.tenor AND y.date < l.date GROUP BY l.tenor)
SELECT l.tenor, l.date, l.value AS yield_pct, ROUND(100 * (l.value - l.prev), 1) AS change_bp,
       ROUND(100 * (l.value - l.prior_year_close), 1) AS ytd_bp, x.highest_since, x.lowest_since
FROM l JOIN x USING (tenor)
