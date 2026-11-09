-- NOAA CPC daily heating (HDD) and cooling (CDD) degree days by census division and contiguous US, 2000 on (~2-day
-- lag), weighted by population, by homes heating with natural gas ('gas' heating = the gas-demand series traders watch)
-- or by electric heat. kind heating / cooling. sum_7d = the last 7 days; value_1y = the same calendar day a year earlier;
-- avg_10y = the same day's average over the 10 prior years; vs_10y = this day minus that average (+ = colder than
-- normal for heating, hotter for cooling). Filter on region = 'CONUS' for the national figure.
WITH s AS (SELECT * , EXTRACT(DAYOFYEAR FROM date) AS doy, EXTRACT(YEAR FROM date) AS yr FROM {{ source('gov', 'degree_days') }}),
n AS (SELECT a.date, a.region, a.kind, a.weights, AVG(b.value) AS avg_10y
      FROM s a JOIN s b ON a.region = b.region AND a.kind = b.kind AND a.weights = b.weights AND b.doy = a.doy
                       AND b.yr BETWEEN a.yr - 10 AND a.yr - 1
      GROUP BY 1, 2, 3, 4)
SELECT s.date, s.region, s.region_name, s.kind, s.weights, s.value,
       SUM(s.value) OVER (PARTITION BY s.region, s.kind, s.weights ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_7d,
       LAG(s.value, 365) OVER (PARTITION BY s.region, s.kind, s.weights ORDER BY s.date) AS value_1y,
       ROUND(n.avg_10y, 1) AS avg_10y, ROUND(s.value - n.avg_10y, 1) AS vs_10y
FROM s LEFT JOIN n USING (date, region, kind, weights)
