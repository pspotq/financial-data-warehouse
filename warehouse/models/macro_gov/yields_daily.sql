-- US Treasury 2Y / 10Y / 30Y yields (percent), one row per market day since 1962. Before 1990 from the history
-- table; from 1990 the official Treasury series the rrp-market job loads
SELECT date, y2, y10, y30, 'history' AS source FROM {{ source('markets', 'history_yields') }} WHERE date < DATE '1990-01-01'
UNION ALL
SELECT obs_date AS date,
       MAX(IF(label = 'UST2Y', close, NULL)) AS y2,
       MAX(IF(label = 'UST10Y', close, NULL)) AS y10,
       MAX(IF(label = 'UST30Y', close, NULL)) AS y30,
       'treasury' AS source
FROM {{ source('markets', 'macro') }}
WHERE label IN ('UST2Y', 'UST10Y', 'UST30Y') AND source = 'treasury' AND obs_date >= DATE '1990-01-01'
GROUP BY obs_date
