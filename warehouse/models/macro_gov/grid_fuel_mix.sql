-- US electricity generation by fuel per day and balancing authority / region (EIA Form 930), Jan 2019 on, MWh.
-- share_pct = the fuel's share of that day's total generation (storage can be negative: charging). Battery, geothermal,
-- pumped storage and solar/wind-with-battery are split out only from Nov 2024; before that they sit in other fuels.
WITH f AS (SELECT period_date AS date, respondent, ANY_VALUE(respondent_name) AS respondent_name, type AS fuel,
                  ANY_VALUE(type_name) AS fuel_name, MAX(value) AS mwh
           FROM {{ source('gov', 'eia_grid') }} WHERE dataset = 'fuel' AND frequency = 'daily' GROUP BY 1, 2, 4)
SELECT *, respondent IN ('US48', 'CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'NW', 'SE', 'SW', 'TEN', 'TEX') AS is_region,
       ROUND(100 * SAFE_DIVIDE(mwh, SUM(mwh) OVER (PARTITION BY date, respondent)), 2) AS share_pct
FROM f
