-- EIA weekly series, one row per series per week (week_ending = the report's period): natural gas storage (lower 48
-- and regions, Bcf; out Thursdays) and petroleum status (crude, gasoline, distillate, Cushing and SPR stocks, production,
-- imports, exports, refinery runs; out Wednesdays), 2000 / 2010 on. change = vs the previous week; change_1y = vs the
-- same ISO week a year earlier; avg_5y = the average for the same ISO week over the 5 prior years, and vs_5y_pct = how
-- far this week sits from it (the comparison the weekly storage report leads with). Units per series in units.
WITH s AS (SELECT *, EXTRACT(ISOWEEK FROM period) AS wk, EXTRACT(ISOYEAR FROM period) AS yr FROM {{ source('gov', 'eia_weekly') }}),
f AS (SELECT a.series, a.period, AVG(b.value) AS avg_5y, COUNT(b.value) AS years_in_avg
      FROM s a JOIN s b ON a.series = b.series AND b.wk = a.wk AND b.yr BETWEEN a.yr - 5 AND a.yr - 1 GROUP BY 1, 2),
y AS (SELECT a.series, a.period, b.value AS value_1y FROM s a JOIN s b ON a.series = b.series AND b.wk = a.wk AND b.yr = a.yr - 1)
SELECT s.period AS week_ending, s.route, s.series, s.series_name, s.area, s.product, s.value, s.units,
       s.value - LAG(s.value) OVER (PARTITION BY s.series ORDER BY s.period) AS change,
       s.value - y.value_1y AS change_1y, ROUND(f.avg_5y, 1) AS avg_5y, f.years_in_avg,
       ROUND(100 * SAFE_DIVIDE(s.value - f.avg_5y, ABS(f.avg_5y)), 2) AS vs_5y_pct
FROM s LEFT JOIN f USING (series, period) LEFT JOIN y USING (series, period)
