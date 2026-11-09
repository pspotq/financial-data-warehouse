-- newest observation per benchmark from its canonical source (rrp.macro_daily role), or from a 'same' source for a
-- date the canonical one has not posted yet (VIX intraday from Yahoo before Cboe posts the close); never a 'reference'
-- source (FRED oil spot, FRED FX noon rates), which until 2026-10-26 could win on a tie. change = vs that source's
-- previous observation. obs_date is the market date the value belongs to
WITH m AS (SELECT *, IF(role = 'canonical', 1, 2) AS pri
           FROM {{ ref('macro_daily') }} WHERE obs_date > DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY) AND close IS NOT NULL
             AND role IN ('canonical', 'same')),
raw AS (SELECT source, series, obs_date, quote_time, ingested_at FROM {{ source('markets', 'macro') }}
        WHERE obs_date > DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY)),
c AS (SELECT m.*, r.quote_time, r.ingested_at,
             LAG(m.close) OVER (PARTITION BY m.source, m.series ORDER BY m.obs_date) AS prev_close,
             LAG(m.obs_date) OVER (PARTITION BY m.source, m.series ORDER BY m.obs_date) AS prev_date
      FROM m LEFT JOIN raw r USING (source, series, obs_date)),
best AS (SELECT * FROM c QUALIFY ROW_NUMBER() OVER (PARTITION BY label ORDER BY obs_date DESC, pri) = 1)
SELECT label, obs_date, close, prev_date, prev_close, ROUND(close - prev_close, 4) AS change,
       ROUND(100 * (close / prev_close - 1), 2) AS change_pct, source, series, quote_time,
       DATE_DIFF(CURRENT_DATE('America/New_York'), obs_date, DAY) AS age_days, ingested_at, role, measure
FROM best
